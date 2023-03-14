# Copyright (C) 2021-2023 tiksan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import datetime
import math
import random
import re
import time

import celery
from mongoengine import QuerySet
from mongoengine.queryset.visitor import Q

from tornium_celery.tasks.api import discordpost, tornget
from tornium_commons import rds
from tornium_commons.errors import DiscordError, NetworkingError
from tornium_commons.formatters import rel_time, torn_timestamp
from tornium_commons.models import NotificationModel, ServerModel, UserModel
from tornium_commons.skyutils import SKYNET_INFO

_TRAVEL_DESTINATIONS = {
    # Destination: [Standard, Airstrip, WLT, BCT]
    "Mexico": [1560, 1080, 780, 480],
    "Cayman Islands": [2100, 1500, 1080, 660],
    "Canada": [2460, 1740, 1200, 720],
    "Hawaii": [8040, 5460, 4020, 2400],
    "United Kingdom": [9540, 6660, 4800, 2880],
    "Argentina": [10020, 7020, 4980, 3000],
    "Switzerland": [10500, 7380, 5280, 3180],
    "Japan": [13500, 9480, 6780, 4080],
    "China": [14520, 10140, 7260, 4320],
    "United Arab Emirates": [16260, 11400, 8100, 4860],
    "South Africa": [17820, 12480, 8940, 5340],
}


def send_notification(notification: NotificationModel, payload: dict):
    if not notification.options["enabled"]:
        return

    if notification.recipient_type == 0:
        try:
            dm_channel = discordpost(
                "users/@me/channels",
                payload={
                    "recipient_id": notification.recipient,
                },
            )
        except DiscordError:
            return
        except NetworkingError:
            return

        discordpost.delay(
            endpoint=f"channels/{dm_channel['id']}/messages", payload=payload, bucket=f"channels/{dm_channel['id']}"
        ).forget()
    elif notification.recipient_type == 1:
        discordpost.delay(
            endpoint=f"channels/{notification.recipient}/messages",
            payload=payload,
            bucket=f"channels/{notification.recipient}",
        ).forget()
    else:
        return

    if not notification.persistent:
        notification.delete()


def get_destination(status_description: str):
    if status_description.startswith("Traveling"):
        return " ".join(status_description.split(" ")[2:])
    elif status_description.startswith("Returning"):
        return " ".join(status_description.split(" ")[4:])
    else:
        raise AttributeError


def first_landing(duration):
    return torn_timestamp(int(time.time()) + math.ceil(duration * 0.97))


@celery.shared_task(routing_key="quick.stakeouts.run_user", queue="quick")
def run_user_stakeouts():
    notifications: QuerySet = NotificationModel.objects(Q(ntype=1) & Q(options__enabled=True))

    notification: NotificationModel
    for notification in notifications:
        invoker: UserModel = UserModel.objects(tid=notification.invoker).first()

        if invoker is None or invoker.key in ("", None):
            if notification.recipient_type == 1:
                guild: ServerModel = ServerModel.objects(sid=notification.recipient_guild).first()

                if guild is None or len(guild.admins) == 0:
                    continue

                key_user: UserModel = UserModel.objects(tid=random.choice(guild.admins)).first()

                if key_user is None:
                    continue

                key = key_user.key
            else:
                continue
        else:
            key = invoker.key

        if key in ("", None):
            continue

        tornget.signature(
            kwargs={
                "endpoint": f"user/{notification.target}?selections=",
                "key": key,
            },
            queue="api",
        ).apply_async(expires=300, link=user_hook.s())


@celery.shared_task(routing_key="quick.stakeouts.user_hook", queue="quick")
def user_hook(user_data):
    if "player_id" not in user_data:
        return

    notifications: QuerySet = NotificationModel.objects(
        Q(invoker=user_data["player_id"]) & Q(ntype=1) & Q(options__enabled=True)
    )

    if notifications.count() == 0:
        return

    redis_key = f"tornium:stakeout-data:user:{user_data['player_id']}"
    redis_client = rds()

    if "name" not in user_data:
        user: UserModel = UserModel.objects(tid=user_data["player_id"]).first()

        if user is None:
            user_data["name"] = "Unknown"
        else:
            user_data["name"] = user.name

    if "last_action" in user_data:
        if 300 < int(time.time()) - user_data["last_action"]["timestamp"] < 360:
            payload = {
                "embeds": [
                    {
                        "title": f"{user_data['name']} Status Change",
                        "description": (
                            f"{user_data['name']} [{user_data['player_id']}] has now been inactive since "
                            f"{rel_time(user_data['last_action']['timestamp'])}"
                        ),
                        "color": SKYNET_INFO,
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": torn_timestamp()},
                    }
                ]
            }

            notification: NotificationModel
            for notification in notifications:
                if 1 not in notification.value:
                    continue
                elif not notification.options["enabled"]:
                    continue

                send_notification(notification, payload)
        elif (
            redis_client.exists(redis_key + ":last_action:timestamp")
            and int(time.time()) - int(redis_client.get(redis_key + ":last_action:timestamp")) > 300
            and int(time.time()) - user_data["last_action"]["timestamp"] <= 300
        ):
            payload = {
                "embeds": [
                    {
                        "title": f"{user_data['name']} Status Change",
                        "description": f"{user_data['name']} [{user_data['player_id']}] is now active.",
                        "color": SKYNET_INFO,
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": torn_timestamp()},
                    }
                ]
            }

            notification: NotificationModel
            for notification in notifications:
                if 0 not in notification.value:
                    continue
                elif not notification.options["enabled"]:
                    continue

                send_notification(notification, payload)

    if "status" in user_data and redis_client.exists(redis_key + ":status:description"):
        description = redis_client.get(redis_key + ":status:description")

        if re.match(r"(Traveling|Returning)", user_data["status"]["description"]) and not re.match(
            r"(Traveling|Returning)", description
        ):
            payload = {
                "embeds": [
                    {
                        "title": f"{user_data['name']} is Flying",
                        "description": (
                            f"{user_data['name']} [{user_data['player_id']}] is now "
                            f"{user_data['status']['description'].lower()}."
                        ),
                        "color": SKYNET_INFO,
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": torn_timestamp()},
                    }
                ]
            }

            notification: NotificationModel
            for notification in notifications:
                if 2 not in notification.value:
                    continue
                elif not notification.options["enabled"]:
                    continue

                send_notification(notification, payload)
        elif not re.match(r"(Traveling|Returning)", user_data["status"]["description"]) and re.match(
            r"(Traveling|Returning)", description
        ):
            if user_data["status"]["state"] != "Abroad":
                description_suffix = f"has returned to Torn"
            else:
                description_suffix = f"has landed {user_data['status']['description'].lower()}"

            destination_durations = _TRAVEL_DESTINATIONS[get_destination(user_data["status"]["description"])]

            payload = {
                "embeds": [
                    {
                        "title": f"{user_data['name']} has Landed",
                        "description": f"{user_data['name']} [{user_data['player_id']}] {description_suffix}.",
                        "color": SKYNET_INFO,
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": torn_timestamp()},
                    }
                ],
                "components": [
                    {
                        "type": 1,
                        "components": [
                            {
                                "type": 2,
                                "style": 2,
                                "label": f"Standard: {first_landing(destination_durations[0])}",
                                "disabled": True,
                            },
                            {
                                "type": 2,
                                "style": 2,
                                "label": f"Airstrip: {first_landing(destination_durations[1])}",
                                "disabled": True,
                            },
                        ],
                    },
                    {
                        "type": 1,
                        "components": [
                            {
                                "type": 2,
                                "style": 2,
                                "label": f"WLT: {first_landing(destination_durations[2])}",
                                "disabled": True,
                            },
                            {
                                "type": 2,
                                "style": 2,
                                "label": f"BCT: {first_landing(destination_durations[3])}",
                                "disabled": True,
                            },
                        ],
                    },
                ],
            }

            notification: NotificationModel
            for notification in notifications:
                if 2 not in notification.value:
                    continue
                elif not notification.options["enabled"]:
                    continue

                send_notification(notification, payload)

        if user_data["status"]["description"] == "Okay" and re.match(r"In (Hospital|Jail).*", description):
            payload = {
                "embeds": [
                    {
                        "title": f"{user_data['name']} is Okay",
                        "description": (
                            f"{user_data['name']} [{user_data['player_id']}] is now okay after being in the "
                            f"{'hospital' if re.match(r'In Hospital.*', description) else 'jail'}."
                        ),
                        "color": SKYNET_INFO,
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": torn_timestamp()},
                    }
                ]
            }

            notification: NotificationModel
            for notification in notifications:
                if 3 not in notification.value:
                    continue
                elif not notification.options["enabled"]:
                    continue

                send_notification(notification, payload)

        if re.match(r"In Hospital.*", user_data["status"]["description"]) and not re.match(
            r"In Hospital.*", description
        ):
            if re.match(
                r"(Hospitalized|Dropped|Shot|Mauled|Taken|Burned|Attacked|Mugged|Kicked|Suffering)",
                user_data["status"]["description"],
            ):
                payload_description = f"have been {user_data['status']['description'].lower()}"
            elif re.match(r"Was shot.*", user_data["status"]["description"]):
                # Valid hosp reasons
                # Was shot while resisting arrest
                payload_description = user_data["status"]["description"].lower().replace("was shot", "have been shot")
            elif re.match(r"Got.*", user_data["status"]["description"]):
                # Valid hosp reasons
                # Got a nasty surprise in the post
                payload_description = user_data["status"]["description"].lower().replace("got", "have gotten")
            else:
                # Valid hosp reasons
                # Overdosed on [Drug]
                # Crashed his [Car]
                # Exploded
                # Lost to [User]
                payload_description = f"have {user_data['status']['description'].lower()}"

            payload = {
                "embeds": [
                    {
                        "title": f"{user_data['name']} is Hospitalized",
                        "description": (
                            f"{user_data['name']} [{user_data['player_id']}] has entered the hospital as "
                            f"they {payload_description}."
                        ),
                        "color": SKYNET_INFO,
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": torn_timestamp()},
                    }
                ]
            }

            notification: NotificationModel
            for notification in notifications:
                if 4 not in notification.value:
                    continue
                elif not notification.options["enabled"]:
                    continue

                send_notification(notification, payload)

    if "last_action" in user_data:
        redis_client.set(redis_key + ":last_action:status", user_data["last_action"]["status"], ex=300)
        redis_client.set(redis_key + ":last_action:timestamp", user_data["last_action"]["timestamp"], ex=300)

    if "status" in user_data:
        redis_client.set(redis_key + ":status:description", user_data["status"]["description"], ex=300)
        redis_client.set(redis_key + ":status:state", user_data["status"]["state"], ex=300)
        redis_client.set(redis_key + ":status:until", user_data["status"]["until"], ex=300)

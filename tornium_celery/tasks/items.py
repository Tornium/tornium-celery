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
import json
import random
import typing

import celery
import playhouse.postgres_ext
from celery.utils.log import get_task_logger
from peewee import chunked, fn
from tornium_commons import db, rds
from tornium_commons.formatters import commas, torn_timestamp
from tornium_commons.models import Item, Notification, Server, User
from tornium_commons.skyutils import SKYNET_INFO

from .api import tornget
from .stakeout_hooks import send_notification

try:
    import orjson

    globals()["orjson:loaded"] = True
except ImportError:
    globals()["orjson:loaded"] = False

logger = get_task_logger("celery_app")


@celery.shared_task(
    name="tasks.items.update_items",
    routing_key="default.items.update_items",
    queue="default",
    time_limit=15,
)
def update_items(items_data):
    Item.update_items(torn_get=tornget, key=User.random_key())

    rds().set(
        "tornium:items:last-update",
        int(datetime.datetime.utcnow().timestamp()),
        ex=5400,
    )  # 1.5 hours


@celery.shared_task(
    name="tasks.items.fetch_market",
    routing_key="default.items.fetch_market",
    queue="default",
    time_limit=15,
)
def fetch_market():
    notifications = Notification.select().where((Notification.n_type == 3) & (Notification.enabled == True))
    unique_items = list(notifications.distinct(Notification.target))

    for item_id in unique_items:
        item_notifications = notifications.where(Notification.target == item_id)

        if item_notifications.first().recipient_type == 0:
            recipient = item_notifications.first().recipient
            key_user: typing.Optional[User] = User.select().where(User.discord_id == recipient).first()
        else:
            recipient = item_notifications.first().recipient_guild
            guild: typing.Optional[Server] = Server.select().where(Server.sid == recipient).first()

            if guild is None:
                item_notifications.delete_instance()
                continue

            key_user: typing.Optional[User] = User.select().where(User.tid == random.choice(guild.admins)).first()

        if key_user is None:
            continue
        elif key_user.key in ("", None):
            continue

        if globals()["orjson:loaded"]:
            notifications_dict = orjson.loads(item_notifications.to_json())
        else:
            notifications_dict = json.loads(item_notifications.to_json())

        tornget.signature(
            kwargs={
                "endpoint": f"market/{item_id}?selections=itemmarket,bazaar",
                "key": key_user.key,
            },
            queue="api",
        ).apply_async(
            expires=300,
            link=market_notifications.signature(kwargs={"notifications": notifications_dict}),
        )


@celery.shared_task(
    name="tasks.items.market_notifications",
    routing_key="default.items.market_notifications",
    queue="default",
    time_limit=15,
)
def market_notifications(market_data: dict, notifications: dict):
    # TODO: Needs to be rewritten as relies on loading and dumping the model via JSON
    return

    if len(notifications) == 0:
        return

    item: Item = Item.select().get_by_id(notifications[0]["target"])

    components = [
        {
            "type": 1,
            "components": [
                {
                    "type": 2,
                    "style": 5,
                    "label": "Item Market",
                    "url": f"https://www.torn.com/imarket.php#/p=shop&step=shop&type=&searchname={item.tid}",
                }
            ],
        }
    ]

    percent_enabled: bool = any(n["options"]["type"] == "percent" for n in notifications)
    price_enabled: bool = any(n["options"]["type"] == "price" for n in notifications)
    quantity_enabled: bool = any(n["options"]["type"] == "quantity" for n in notifications)
    redis_client = rds()

    if market_data["itemmarket"] is None:
        market_data["itemmarket"] = []
    if market_data["bazaar"] is None:
        market_data["bazaar"] = []

    if not redis_client.exists(f"tornium:items:{item.tid}:ids"):
        market_ids = [listing["ID"] for listing in market_data["itemmarket"] + market_data["bazaar"]]

        if len(market_ids) != 0:
            redis_client.sadd(f"tornium:items:{item.tid}:ids", *market_ids)
        return

    item_ids = redis_client.smembers(f"tornium:items:{item.tid}:ids")

    if percent_enabled:
        notifications_map = {}

        for listing in market_data["itemmarket"] + market_data["bazaar"]:
            if listing["cost"] >= item.market_value:
                continue
            elif str(listing["ID"]) in item_ids:
                continue

            percent_change = abs(listing["cost"] - item.market_value) / item.market_value * 100

            for n in notifications:
                if n["options"]["type"] != "percent" or n["value"] > percent_change:
                    continue

                if n["_id"]["$oid"] not in notifications:
                    notifications_map[n["_id"]["$oid"]] = []

                notifications_map[n["_id"]["$oid"]].append(
                    {
                        "cost": listing["cost"],
                        "quantity": listing["quantity"],
                        "percent_change": percent_change,
                    }
                )

        notif: dict
        for notif in notifications:
            if n["_id"]["$oid"] not in notifications_map:
                continue

            fields = []
            i = 1
            total_quantity = 0

            for listing in notifications_map[n["_id"]["$oid"]]:
                fields.append(
                    {
                        "name": f"Bazaar #{i}",
                        "value": f"{commas(listing['quantity'])}x @ ${commas(listing['cost'])} → {commas(listing['quantity'] * listing['cost'])} @ -{listing['percent_change']}%",
                        "inline": True,
                    }
                )
                total_quantity += listing["quantity"]
                i += 1

            send_notification(
                n_db,
                payload={
                    "embeds": [
                        {
                            "title": f"{item.name} Percent Change Notification",
                            "description": f"{commas(total_quantity)}x of {item.name} have been found in {i - 1} bazaar(s) or market listing(s).",
                            "fields": fields,
                            "color": SKYNET_INFO,
                            "timestamp": datetime.datetime.utcnow().isoformat(),
                            "footer": {"text": torn_timestamp()},
                        }
                    ],
                    "components": components,
                },
            )

    if price_enabled:
        notifications_map = {}

        for listing in market_data["itemmarket"] + market_data["bazaar"]:
            if str(listing["ID"]) in item_ids:
                continue

            for n in notifications:
                if n["options"]["type"] != "price" or listing["cost"] >= n["value"]:
                    continue

                if n["_id"]["$oid"] not in notifications:
                    notifications_map[n["_id"]["$oid"]] = []

                notifications_map[n["_id"]["$oid"]].append(
                    {
                        "cost": listing["cost"],
                        "quantity": listing["quantity"],
                    }
                )

        for n in notifications:
            if n["_id"]["$oid"] not in notifications_map:
                continue

            notification_str = orjson.dumps(n) if globals()["orjson:loaded"] else json.dumps(n)
            n_db = NotificationModel.from_json(notification_str)
            fields = []
            i = 1
            total_quantity = 0

            if item.market_value == 0:
                continue

            for listing in notifications_map[n["_id"]["$oid"]]:
                percent_change = round((listing["cost"] - item.market_value) / item.market_value * 100, 1)
                fields.append(
                    {
                        "name": f"Bazaar #{i}",
                        "value": f"{commas(listing['quantity'])}x @ ${commas(listing['cost'])} → ${commas(listing['quantity'] * listing['cost'])} ({percent_change}%)",
                        "inline": True,
                    }
                )
                total_quantity += listing["quantity"]
                i += 1

            send_notification(
                n_db,
                payload={
                    "embeds": [
                        {
                            "title": f"{item.name} Price Notification",
                            "description": f"{commas(total_quantity)}x of {item.name} have been found in {i - 1} bazaar(s) or market listing(s).",
                            "fields": fields,
                            "color": SKYNET_INFO,
                            "timestamp": datetime.datetime.utcnow().isoformat(),
                            "footer": {"text": torn_timestamp()},
                        }
                    ],
                    "components": components,
                },
            )

    if quantity_enabled:
        total_quantity = sum(listing["quantity"] for listing in market_data["bazaar"][:3])

        for n in notifications:
            if n["options"]["type"] != "quantity" or n["value"] < total_quantity:
                continue

            notification_str = orjson.dumps(n) if globals()["orjson:loaded"] else json.dumps(n)
            n_db = NotificationModel.from_json(notification_str)

            fields = []
            i = 1

            for listing in market_data["bazaar"][:3]:
                percent_change = round((listing["cost"] - item.market_value) / item.market_value * 100, 1)

                fields.append(
                    {
                        "name": f"Bazaar #{i}",
                        "value": f"{commas(listing['quantity'])}x @ ${commas(listing['cost'])} → ${commas(listing['quantity'] * listing['cost'])} ({percent_change}%)",
                        "inline": True,
                    }
                )
                i += 1

            send_notification(
                n_db,
                payload={
                    "embeds": [
                        {
                            "title": f"{item.name} Quantity Notification",
                            "description": f"{commas(total_quantity)}x of {item.name} have been found in the first "
                            f"three visible bazaars.",
                            "fields": fields,
                            "color": SKYNET_INFO,
                            "timestamp": datetime.datetime.utcnow().isoformat(),
                            "footer": {"text": torn_timestamp()},
                        }
                    ],
                    "components": components,
                },
            )

    redis_client.delete(f"tornium:items:{item.tid}:ids")
    market_ids = [listing["ID"] for listing in market_data["itemmarket"] + market_data["bazaar"]]

    if len(market_ids) != 0:
        redis_client.sadd(f"tornium:items:{item.tid}:ids", *market_ids)

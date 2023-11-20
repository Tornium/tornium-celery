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
import time
import typing
import uuid
from decimal import DivisionByZero

import celery
from celery.utils.log import get_task_logger
from peewee import JOIN, DoesNotExist
from tornium_commons import rds
from tornium_commons.errors import DiscordError, NetworkingError, TornError
from tornium_commons.formatters import commas, torn_timestamp
from tornium_commons.models import (
    Faction,
    FactionPosition,
    Item,
    OrganizedCrime,
    Server,
    Stat,
    User,
    Withdrawal,
)
from tornium_commons.skyutils import SKYNET_ERROR, SKYNET_GOOD

from .api import discordpatch, discordpost, torn_stats_get, tornget
from .misc import send_dm
from .user import update_user

logger = get_task_logger("celery_app")

ORGANIZED_CRIMES = {
    1: "Blackmail",
    2: "Kidnapping",
    3: "Bomb Threat",
    4: "Planned Robbery",
    5: "Rob a Money Train",
    6: "Take over a Cruise Liner",
    7: "Hijack a Plane",
    8: "Political Assassination",
}

ATTACK_RESULTS = {
    "Lost": 0,
    "Attacked": 1,
    "Mugged": 2,
    "Hospitalized": 3,
    "Stalemate": 4,
    "Escape": 5,
    "Assist": 6,
    "Special": 7,
    "Looted": 8,
    "Arrested": 9,
    "Timeout": 10,
    "Interrupted": 11,
}


@celery.shared_task(
    name="tasks.faction.refresh_factions",
    routing_key="default.refresh_factions",
    queue="default",
)
def refresh_factions():
    faction: Faction
    for faction in Faction.select().join(Server):
        leader: typing.Optional[User] = User.select(User.key).where(User.tid == faction.leader_id).first()
        coleader: typing.Optional[User] = User.select(User.key).where(User.tid == faction.coleader_id).first()
        aa_keys: typing.Set[str] = set()

        if leader is not None and leader.key not in (None, ""):
            aa_keys.add(leader.key)
        if coleader is not None and coleader.key not in (None, ""):
            aa_keys.add(coleader.key)

        aa_keys = aa_keys.union(
            {
                u.key
                for u in User.select(User.key).where(
                    (User.key.is_null(False))
                    & (User.key != "")
                    & (User.faction_id == faction.tid)
                    & (User.faction_aa == True)  # noqa 712
                )
            }
        )
        aa_keys = [k for k in aa_keys if k not in (None, "")]
        Faction.update(aa_keys=aa_keys).where(Faction.tid == faction.tid).execute()

        if len(aa_keys) == 0:
            continue

        tornget.signature(
            kwargs={
                "endpoint": "faction/?selections=basic,positions",
                "key": random.choice(aa_keys),
            },
            queue="api",
        ).apply_async(expires=300, link=update_faction.s())

        ts_key = ""

        if leader is not None and leader.key not in ("", None):
            ts_key = leader.key
        else:
            if coleader is not None and coleader.key not in ("", None):
                ts_key = coleader.key

        if ts_key != "":
            torn_stats_get.signature(
                kwargs={"endpoint": f"spy/faction/{faction.tid}", "key": ts_key},
                expires=300,
                link=update_faction_ts.s(),
            )

        if faction.od_channel != 0 and faction.guild not in (None, 0):
            try:
                tornget.signature(
                    kwargs={
                        "endpoint": "faction/?selections=basic,contributors",
                        "stat": "drugoverdoses",
                        "key": random.choice(aa_keys),
                    },
                    queue="api",
                ).apply_async(
                    expires=300,
                    link=check_faction_ods.s(),
                )
            except Exception as e:
                logger.exception(e)
                continue


@celery.shared_task(
    name="tasks.faction.update_faction",
    routing_key="quick.update_faction",
    queue="quick",
)
def update_faction(faction_data):
    if faction_data is None:
        return
    elif faction_data.get("ID") is None:
        return  # Must include faction/basic

    Faction.insert(
        tid=faction_data["ID"],
        name=faction_data["name"],
        tag=str(faction_data["tag"]),  # Torn occasionally uses integers as tags
        respect=faction_data["respect"],
        capacity=faction_data["capacity"],
        leader=User.select().where(User.tid == faction_data["leader"]).first(),
        coleader=User.select().where(User.tid == faction_data["co-leader"]).first()
        if faction_data["co-leader"] != 0
        else None,
        last_members=datetime.datetime.utcnow(),
    ).on_conflict(
        conflict_target=[Faction.tid],
        preserve=[
            Faction.name,
            Faction.tag,
            Faction.respect,
            Faction.capacity,
            Faction.leader,
            Faction.coleader,
            Faction.last_members,
        ],
    ).execute()

    # faction/positions
    if "positions" in faction_data:
        positions_data = update_faction_positions(faction_data)

    users = [member_id for member_id in faction_data["members"].keys()]

    for member_id, member in faction_data["members"].items():
        if "positions" in faction_data:
            User.insert(
                tid=int(member_id),
                name=member["name"],
                level=member["level"],
                faction=faction_data["ID"],
                faction_aa=positions_data[member["position"]]["aa"] if member["position"] is not None else False,
                faction_position=positions_data[member["position"]]["uuid"] if member["position"] is not None else None,
                status=member["last_action"]["status"],
                last_action=datetime.datetime.fromtimestamp(
                    member["last_action"]["timestamp"], tz=datetime.timezone.utc
                ),
                last_refresh=datetime.datetime.utcnow(),
            ).on_conflict(
                conflict_target=[User.tid],
                preserve=[
                    User.name,
                    User.level,
                    User.faction,
                    User.faction_aa,
                    User.status,
                    User.last_action,
                    User.last_refresh,
                ],
            ).execute()
        else:
            User.insert(
                tid=member["player_id"],
                name=member["name"],
                level=member["level"],
                faction=faction_data["ID"],
                status=member["last_action"]["status"],
                last_action=datetime.datetime.fromtimestamp(
                    member["last_action"]["timestamp"], tz=datetime.timezone.utc
                ),
                last_refresh=datetime.datetime.utcnow(),
            ).on_conflict(
                conflict_target=[User.tid],
                preserve=[
                    User.name,
                    User.level,
                    User.faction,
                    User.status,
                    User.last_action,
                    User.last_refresh,
                ],
            ).execute()

    leader: typing.Optional[User] = User.select(User.key).where(User.tid == faction_data["leader"]).first()
    coleader: typing.Optional[User] = (
        User.select(User.key).where(User.tid == faction_data["co-leader"]).first()
        if faction_data["co-leader"] != 0
        else None
    )
    aa_keys: typing.Set[str] = set()

    if leader is not None and leader.key not in (None, ""):
        aa_keys.add(leader.key)
    if coleader is not None and coleader.key not in (None, ""):
        aa_keys.add(coleader.key)

    aa_keys = aa_keys.union(
        {
            u.key
            for u in User.select(User.key).where(
                (User.key.is_null(False))
                & (User.key != "")
                & (User.faction_id == faction_data["ID"])
                & (User.faction_aa == True)  # noqa 712
            )
        }
    )
    aa_keys = [k for k in aa_keys if k not in (None, "")]
    Faction.update(aa_keys=list(aa_keys)).where(Faction.tid == faction_data["ID"]).execute()

    # Strips old faction members of their faction data
    User.update(faction=None, faction_position=None, faction_aa=False).where(
        (User.faction_id == faction_data["ID"]) & (User.tid.not_in(users))
    ).execute()


@celery.shared_task(
    name="tasks.faction.update_faction_positions",
    routing_key="quick.update_faction_positions",
    queue="quick",
)
def update_faction_positions(faction_positions_data: dict) -> typing.Optional[dict]:
    if "positions" not in faction_positions_data or "ID" not in faction_positions_data:
        return None

    existing_positions = FactionPosition.select(FactionPosition.pid, FactionPosition.name).where(
        FactionPosition.faction_tid == faction_positions_data["ID"]
    )
    existing_position_names: typing.Set[str] = {position.name for position in existing_positions}

    latest_position_names: typing.Set[str] = {k for k in faction_positions_data["positions"]}

    positions_data = {
        "Recruit": {
            "uuid": None,
            "aa": False,
        },
        "Leader": {
            "uuid": None,
            "aa": True,
        },
        "Co-leader": {
            "uuid": None,
            "aa": True,
        },
    }

    deleted_position_name: str
    for deleted_position_name in existing_position_names - latest_position_names:
        try:
            existing_positions.where(FactionPosition.name == deleted_position_name).delete_instance()
        except Exception as e:
            logger.exception(e)
            continue

        existing_position_names.remove(deleted_position_name)

    add_position_name: str
    for add_position_name in latest_position_names - existing_position_names:
        perms = faction_positions_data["positions"][add_position_name]
        pid = uuid.uuid4().hex

        FactionPosition.insert(
            pid=pid,
            name=add_position_name,
            faction_tid=faction_positions_data["ID"],
            default=bool(perms["default"]),
            use_medical_item=bool(perms["canUseMedicalItem"]),
            use_booster_item=bool(perms["canUseBoosterItem"]),
            use_drug_item=bool(perms["canUseDrugItem"]),
            use_energy_refill=bool(perms["canUseEnergyRefill"]),
            use_nerve_refill=bool(perms["canUseNerveRefill"]),
            loan_temporary_item=bool(perms["canLoanTemporaryItem"]),
            loan_weapon_armory=bool(perms["canLoanWeaponAndArmory"]),
            retrieve_loaned_armory=bool(perms["canRetrieveLoanedArmory"]),
            plan_init_oc=bool(perms["canPlanAndInitiateOrganisedCrime"]),
            access_fac_api=bool(perms["canAccessFactionApi"]),
            give_item=bool(perms["canGiveItem"]),
            give_money=bool(perms["canGiveMoney"]),
            give_points=bool(perms["canGivePoitns"]),
            manage_forums=bool(perms["canManageForum"]),
            manage_applications=bool(perms["canManageApplications"]),
            kick_members=bool(perms["canKickMembers"]),
            adjust_balances=bool(perms["canAdjustMemberBalance"]),
            manage_wars=bool(perms["canManageWars"]),
            manage_upgrades=bool(perms["canManageUpgrades"]),
            send_newsletters=bool(perms["canSendNewsletter"]),
            change_announcement=bool(perms["canChangeAnnouncement"]),
            change_description=bool(perms["canChangeDescription"]),
        ).on_conflict(
            conflict_target=[FactionPosition.pid],
            preserve=[
                getattr(FactionPosition, p)
                for p in set(FactionPosition._meta.sorted_field_names) - {"pid", "name", "faction_tid"}
            ],
        ).execute()

        existing_position_names.add(add_position_name)
        positions_data[add_position_name] = {
            "uuid": pid,
            "aa": bool(perms["canAccessFactionApi"]),
        }

    modify_position_name: str
    for modify_position_name in existing_position_names & latest_position_names:
        perms = faction_positions_data["positions"][modify_position_name]
        existing_position: typing.Optional[FactionPosition] = existing_positions.where(
            FactionPosition.name == modify_position_name
        ).first()

        if existing_position is None:
            continue

        FactionPosition.insert(
            pid=existing_position.pid,
            name=modify_position_name,
            faction_tid=faction_positions_data["ID"],
            default=bool(perms["default"]),
            use_medical_item=bool(perms["canUseMedicalItem"]),
            use_booster_item=bool(perms["canUseBoosterItem"]),
            use_drug_item=bool(perms["canUseDrugItem"]),
            use_energy_refill=bool(perms["canUseEnergyRefill"]),
            use_nerve_refill=bool(perms["canUseNerveRefill"]),
            loan_temporary_item=bool(perms["canLoanTemporaryItem"]),
            loan_weapon_armory=bool(perms["canLoanWeaponAndArmory"]),
            retrieve_loaned_armory=bool(perms["canRetrieveLoanedArmory"]),
            plan_init_oc=bool(perms["canPlanAndInitiateOrganisedCrime"]),
            access_fac_api=bool(perms["canAccessFactionApi"]),
            give_item=bool(perms["canGiveItem"]),
            give_money=bool(perms["canGiveMoney"]),
            give_points=bool(perms["canGivePoints"]),
            manage_forums=bool(perms["canManageForum"]),
            manage_applications=bool(perms["canManageApplications"]),
            kick_members=bool(perms["canKickMembers"]),
            adjust_balances=bool(perms["canAdjustMemberBalance"]),
            manage_wars=bool(perms["canManageWars"]),
            manage_upgrades=bool(perms["canManageUpgrades"]),
            send_newsletters=bool(perms["canSendNewsletter"]),
            change_announcement=bool(perms["canChangeAnnouncement"]),
            change_description=bool(perms["canChangeDescription"]),
        ).on_conflict(
            conflict_target=[FactionPosition.pid],
            preserve=[
                getattr(FactionPosition, p)
                for p in set(FactionPosition._meta.sorted_field_names) - {"pid", "name", "faction_tid"}
            ],
        ).execute()

        positions_data[modify_position_name] = {
            "uuid": existing_position.pid,
            "aa": bool(perms["canAccessFactionApi"]),
        }

    return positions_data


@celery.shared_task(
    name="tasks.faction.update_faction_ts",
    routing_key="default.update_faction_ts",
    queue="default",
)
def update_faction_ts(faction_ts_data):
    if not faction_ts_data["status"]:
        return

    for user_id, user_data in faction_ts_data["faction"]["members"].items():
        if "spy" not in user_data:
            continue

        try:
            user: User = User.select().where(User.tid == int(user_id)).get()
        except DoesNotExist:
            continue

        if user.key is not None:
            continue
        elif (
            user.battlescore_update is not None and user_data["spy"]["timestamp"] <= user.battlescore_update.timestamp()
        ):
            continue

        user.battlescore = (
            math.sqrt(user_data["spy"]["strength"])
            + math.sqrt(user_data["spy"]["defense"])
            + math.sqrt(user_data["spy"]["speed"])
            + math.sqrt(user_data["spy"]["dexterity"])
        )
        user.strength = user_data["spy"]["strength"]
        user.defense = user_data["spy"]["defense"]
        user.speed = user_data["spy"]["speed"]
        user.dexterity = user_data["spy"]["dexterity"]
        user.battlescore_update = datetime.datetime.fromtimestamp(
            user_data["spy"]["timestamp"], tz=datetime.timezone.utc
        )
        user.save()


@celery.shared_task(
    name="tasks.faction.check_faction_ods",
    routing_key="quick.check_faction_ods",
    queue="quick",
)
def check_faction_ods(faction_od_data):
    try:
        faction: Faction = Faction.select().join(Server).where(Faction.tid == faction_od_data["ID"]).get()
    except (KeyError, DoesNotExist):
        return

    if len(faction.od_data) == 0:
        faction.od_data = faction_od_data["contributors"]["drugoverdoses"]
        faction.save()
        return
    elif faction.od_channel in (0, None):
        return

    if faction.guild is None:
        faction.od_data = faction_od_data["contributors"]["drugoverdoses"]
        faction.save()
        return

    for tid, user_od in faction_od_data["contributors"]["drugoverdoses"].items():
        if faction.od_data.get(tid) is None and user_od["contributed"] > 0:
            overdosed_user: typing.Optional[User] = User.select(User.name).where(User.tid == tid).first()
            payload = {
                "embeds": [
                    {
                        "title": "User Overdose",
                        "description": f"User {tid if overdosed_user is None else overdosed_user.name} "
                        f"of faction {faction.name} has overdosed.",
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
                                "style": 5,
                                "label": "User",
                                "url": f"https://www.torn.com/profiles.php?XID={tid}",
                            }
                        ],
                    }
                ],
            }

            discordpost.delay(
                f"channels/{faction.od_channel}/messages",
                payload=payload,
                channel=faction.od_channel,
            ).forget()
        elif faction.od_data.get(tid) is not None and user_od["contributed"] != faction.od_data.get(tid).get(
            "contributed"
        ):
            overdosed_user: typing.Optional[User] = User.select(User.name).where(User.tid == tid).first()
            payload = {
                "embeds": [
                    {
                        "title": "User Overdose",
                        "description": f"User {tid if overdosed_user is None else overdosed_user.name} "
                        f"of faction {faction.name} has overdosed.",
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
                                "style": 5,
                                "label": "User",
                                "url": f"https://www.torn.com/profiles.php?XID={tid}",
                            }
                        ],
                    }
                ],
            }

            discordpost.delay(
                f"channels/{faction.od_channel}/messages",
                payload=payload,
                channel=faction.od_channel,
            ).forget()

    faction.od_data = faction_od_data["contributors"]["drugoverdoses"]
    faction.save()


@celery.shared_task(
    name="tasks.faction.fetch_attacks_runner",
    routing_key="quick.fetch_attacks_runner",
    queue="quick",
)
def fetch_attacks_runner():
    redis = rds()

    if (
        redis.exists("tornium:celery-lock:fetch-attacks") and redis.ttl("tornium:celery-lock:fetch-attacks") > 1
    ):  # Lock enabled
        logger.debug("Fetch attacks task terminated due to pre-existing task")
        raise Exception(
            f"Can not run task as task is already being run. Try again in "
            f"{redis.ttl('tornium:celery-lock:fetch-attacks')} seconds."
        )

    if redis.setnx("tornium:celery-lock:fetch-attacks", 1):
        redis.expire("tornium:celery-lock:fetch-attacks", 30)
    if redis.ttl("tornium:celery-lock:fetch-attacks") < 1:
        redis.expire("tornium:celery-lock:fetch-attacks", 1)

    faction: Faction
    for faction in Faction.select().where((Faction.aa_keys != [])):
        if len(faction.aa_keys) == 0:
            continue
        elif faction.last_attacks is None or faction.last_attacks.timestamp() == 0:
            faction.last_attacks = datetime.datetime.utcnow()
            faction.save()
            continue
        elif time.time() - faction.last_attacks.timestamp() > 86401:  # One day
            # Prevents old data from being added (especially for retals)
            faction.last_attacks = datetime.datetime.utcnow()
            faction.save()
            continue

        aa_key = random.choice(faction.aa_keys)
        last_attacks: int = faction.last_attacks.timestamp()

        tornget.signature(
            kwargs={
                "endpoint": "faction/?selections=basic,attacks",
                "fromts": last_attacks + 1,  # timestamp is inclusive
                "key": aa_key,
            },
            queue="api",
        ).apply_async(
            expires=300,
            link=celery.group(
                retal_attacks.signature(
                    kwargs={"last_attacks": last_attacks},
                    queue="quick",
                ),
                stat_db_attacks.signature(
                    kwargs={"last_attacks": last_attacks},
                    queue="quick",
                ),
            ),
        )


@celery.shared_task(name="tasks.faction.retal_attacks", routing_key="quick.retal_attacks", queue="quick")
def retal_attacks(faction_data, last_attacks=None):
    if "attacks" not in faction_data:
        return
    elif len(faction_data["attacks"]) == 0:
        return

    try:
        faction: Faction = Faction.select().join(Server).where(Faction.tid == faction_data["ID"]).get()
    except (KeyError, DoesNotExist):
        return

    if faction.guild is None:
        return
    elif faction.tid not in faction.guild.factions:
        return
    elif str(faction.tid) not in faction.guild.retal_config:
        return

    try:
        if faction.guild.retal_config[str(faction.tid)]["channel"] in (
            "0",
            0,
            None,
            "",
        ):
            return
    except KeyError:
        return

    if last_attacks is None or last_attacks >= time.time():
        last_attacks = faction.last_attacks.timestamp()

    now = int(time.time())

    for attack in faction_data["attacks"].values():
        if attack["result"] in [
            "Assist",
            "Lost",
            "Stalemate",
            "Escape",
            "Looted",
            "Interrupted",
            "Timeout",
        ]:
            continue
        elif attack["defender_id"] in [
            4,
            10,
            15,
            17,
            19,
            20,
            21,
        ]:  # Checks if NPC fight (and you defeated NPC)
            continue
        elif attack["timestamp_ended"] <= last_attacks:
            continue
        elif attack["defender_faction"] != faction.tid:  # Not a defend
            continue
        elif attack["attacker_id"] in ("", 0):  # Stealthed attacker
            continue
        elif attack["respect"] == 0:  # Attack by fac member or recruit
            continue
        elif (
            attack["modifiers"]["overseas"] == 1.25 and attack["modifiers"]["war"] == 1
        ):  # Overseas attack when not in war
            continue
        elif now - attack["timestamp_ended"] >= 300:
            continue

        user: typing.Optional[User] = User.select().where(User.tid == attack["defender_id"]).first()
        opponent: typing.Optional[User] = User.select().where(User.tid == attack["attacker_id"]).first()

        if user is None:
            user = User(
                tid=attack["defender_id"],
                name=attack["defender_name"],
                faction=attack["defender_faction"],
            )
            user.save()

        if opponent is None:
            opponent = User(
                tid=attack["attacker_id"],
                name=attack["attacker_name"],
                faction=attack["attacker_faction"],
            )
            opponent.save()

        if attack["attacker_faction"] == 0:
            title = f"{faction.name} can retal on {opponent.name} [{opponent.tid}]"
        else:
            title = (
                f"{faction.name} can retal on {opponent.name} [{opponent.tid}] from "
                f"{attack['attacker_factionname']} [{attack['attacker_faction']}]"
            )

        fields = [
            {
                "name": "Timeout",
                "value": f"<t:{attack['timestamp_ended'] + 300}:R>",  # Five minutes after attack ends
            }
        ]

        if attack["modifiers"]["fair_fight"] != 3:
            if (
                user is not None
                and user.battlescore != 0
                and user.battlescore_update is not None
                and int(time.time()) - user.battlescore_update.timestamp() <= 259200
            ):  # Three days
                try:
                    opponent_score = user.battlescore / ((attack["modifiers"]["fair_fight"] - 1) * 0.375)
                except DivisionByZero:
                    opponent_score = 0

                if opponent_score != 0:
                    fields.extend(
                        (
                            {
                                "name": "Estimated Stat Score",
                                "value": commas(round(opponent_score)),
                                "inline": True,
                            },
                            {
                                "name": "Stat Score Update",
                                "value": f"<t:{int(time.time())}:R>",
                                "inline": True,
                            },
                        )
                    )
        else:
            stat: typing.Optional[Stat]
            try:
                if user is not None and user.faction.tid is not None:
                    stat = (
                        Stat.select()
                        .where(
                            (Stat.tid == opponent.tid)
                            & ((Stat.added_group == 0) | (Stat.added_group == user.faction_id))
                        )
                        .order_by(Stat.time_added)
                        .first()
                    )
                else:
                    stat = (
                        Stat.select()
                        .where((Stat.tid == opponent.tid) & (Stat.added_group == 0))
                        .order_by(Stat.time_added)
                        .first()
                    )
            except AttributeError as e:
                logger.exception(e),
                stat = None

            if stat is not None:
                fields.extend(
                    (
                        {
                            "name": "Estimated Stat Score",
                            "value": commas(stat.battlescore),
                            "inline": True,
                        },
                        {
                            "name": "Stat Score Update",
                            "value": f"<t:{int(stat.time_added.timestamp())}:R>",
                            "inline": True,
                        },
                    )
                )

        if attack["attacker_faction"] in (0, ""):
            pass
        elif attack["chain"] > 100:
            fields.append(
                {
                    "name": "Opponent Faction Chaining",
                    "value": "True",
                    "inline": False,
                }
            )
        else:
            fields.append({"name": "Opponent Faction Chaining", "value": "False", "inline": False})

        payload = {
            "embeds": [
                {
                    "title": title,
                    "description": f"{opponent.name} [{opponent.tid}] {attack['result'].lower()} {user.name} "
                    f"[{user.tid}] (-{attack['respect_loss']})",
                    "fields": fields,
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
                            "style": 5,
                            "label": "Attack Log",
                            "url": f"https://www.torn.com/loader.php?sid=attackLog&ID={attack['code']}",
                        },
                        {
                            "type": 2,
                            "style": 5,
                            "label": "RETAL!!",
                            "url": f"https://www.torn.com/loader.php?sid=attack&user2ID={opponent.tid}",
                        },
                    ],
                },
                {
                    "type": 1,
                    "components": [
                        {
                            "type": 2,
                            "style": 5,
                            "label": f"{opponent.name}",
                            "url": f"https://www.torn.com/profiles.php?XID={opponent.tid}",
                        },
                        {
                            "type": 2,
                            "style": 5,
                            "label": f"{attack['attacker_factionname']}",
                            "url": f"https://www.torn.com/factions.php?step=profile&userID={opponent.tid}",
                        },
                    ],
                },
            ],
        }

        for role in faction.guild.retal_config[str(faction.tid)]["roles"]:
            if "content" not in payload:
                payload["content"] = ""

            payload["content"] += f"<@&{role}>"

        try:
            discordpost.delay(
                f"channels/{faction.guild.retal_config[str(faction.tid)]['channel']}/messages",
                payload=payload,
            ).forget()
        except Exception as e:
            logger.exception(e)
            continue


@celery.shared_task(
    name="tasks.faction.stat_db_attacks",
    routing_key="quick.stat_db_attacks",
    queue="quick",
)
def stat_db_attacks(faction_data, last_attacks=None):
    if len(faction_data.get("attacks", [])) == 0:
        return

    try:
        faction: Faction = Faction.select().where(Faction.tid == faction_data["ID"]).get()
    except (KeyError, DoesNotExist):
        return

    if not faction.stats_db_enabled:
        return

    if last_attacks is None or last_attacks >= int(time.time()):
        last_attacks = faction.last_attacks

    attack: dict
    for attack in faction_data["attacks"].values():
        if attack["result"] in [
            "Assist",
            "Lost",
            "Stalemate",
            "Escape",
            "Looted",
            "Interrupted",
            "Timeout",
        ]:
            continue
        elif attack["defender_id"] in [
            4,
            10,
            15,
            17,
            19,
            20,
            21,
        ]:  # Checks if NPC fight (and you defeated NPC)
            continue
        elif attack["modifiers"]["fair_fight"] in (
            1,
            3,
        ):  # 3x FF can be greater than the defender battlescore indicated
            continue
        elif attack["timestamp_ended"] <= last_attacks:
            continue

        # User: faction member
        # Opponent: non-faction member regardless of attack or defend

        if attack["defender_faction"] == faction_data["ID"]:  # Defender fac is the fac making the call
            if attack["attacker_id"] in ("", 0):  # Attacker stealthed
                continue
            elif attack["respect"] == 0:  # Attack by fac member
                continue

            user: typing.Optional[User] = (
                User.select(User.battlescore, User.battlescore_update, User.faction)
                .where(User.tid == attack["defender_id"])
                .first()
            )

            if user is None or user.battlescore in (None, 0):
                continue
            elif (
                user.battlescore_update is None or int(time.time()) - user.battlescore_update.timestamp() > 259200
            ):  # Three days
                continue

            opponent_id = attack["attacker_id"]

            if attack["attacker_faction"] != 0:
                Faction.insert(
                    tid=attack["attacker_faction"],
                    name=attack["attacker_factionname"],
                ).on_conflict(
                    conflict_target=[Faction.tid],
                    preserve=[Faction.name],
                ).execute()

            User.insert(
                tid=attack["attacker_id"],
                name=attack["attacker_name"],
                faction=attack["attacker_faction"] if attack["attacker_faction"] != 0 else None,
            ).on_conflict(
                conflict_target=[User.tid],
                preserve=[User.name, User.faction],
            ).execute()
        else:  # User is the attacker
            user: typing.Optional[User] = (
                User.select(User.battlescore, User.battlescore_update, User.faction)
                .where(User.tid == attack["attacker_id"])
                .first()
            )

            if user is None or user.battlescore in (None, 0):
                continue
            elif (
                user.battlescore_update is None or int(time.time()) - user.battlescore_update.timestamp() > 259200
            ):  # Three days
                continue

            opponent_id = attack["defender_id"]

            if attack["defender_faction"] != 0:
                Faction.insert(
                    tid=attack["defender_faction"],
                    name=attack["defender_factionname"],
                ).on_conflict(
                    conflict_target=[Faction.tid],
                    preserve=[Faction.name],
                ).execute()

            User.insert(
                tid=attack["defender_id"],
                name=attack["defender_name"],
                faction=attack["defender_faction"] if attack["defender_faction"] != 0 else None,
            ).on_conflict(
                conflict_target=[User.tid],
                preserve=[User.name, User.faction],
            ).execute()

        try:
            update_user.delay(tid=opponent_id, key=random.choice(faction.aa_keys)).forget()
        except Exception as e:
            logger.exception(e)
            continue

        try:
            if attack["defender_faction"] == faction_data["ID"]:
                opponent_score = user.battlescore / ((attack["modifiers"]["fair_fight"] - 1) * 0.375)
            else:
                opponent_score = (attack["modifiers"]["fair_fight"] - 1) * 0.375 * user.battlescore
        except DivisionByZero:
            continue

        if opponent_score == 0:
            continue

        try:
            Stat.create(
                tid=opponent_id,
                battlescore=opponent_score,
                time_added=datetime.datetime.fromtimestamp(attack["timestamp_ended"], tz=datetime.timezone.utc),
                added_group=0 if faction.stats_db_global else user.faction_id,
            )
        except Exception as e:
            logger.exception(e)
            continue

    Faction.update(
        last_attacks=datetime.datetime.fromtimestamp(
            list(faction_data["attacks"].values())[-1]["timestamp_ended"], tz=datetime.timezone.utc
        )
    ).where(Faction.tid == faction_data["ID"]).execute()


@celery.shared_task(name="tasks.faction.oc_refresh", routing_key="quick.oc_refresh", queue="quick")
def oc_refresh():
    faction: Faction
    for faction in Faction.select().join(Server).where(Faction.aa_keys != []):
        if len(faction.aa_keys) == 0:
            continue
        elif faction.guild is None:
            continue
        elif faction.tid not in faction.guild.factions:
            continue
        elif str(faction.tid) not in faction.guild.oc_config:
            continue

        aa_key = random.choice(faction.aa_keys)

        tornget.signature(
            kwargs={
                "endpoint": "faction/?selections=basic,crimes",
                "key": aa_key,
            },
            queue="api",
        ).apply_async(
            expires=300,
            link=oc_refresh_subtask.s(),
        )


@celery.shared_task(
    name="tasks.faction.oc_refresh_subtask",
    routing_key="default.oc_refresh_subtask",
    queue="default",
)
def oc_refresh_subtask(oc_data):  # TODO: Refactor this to be more readable
    try:
        faction: Faction = Faction.select().join(Server).where(Faction.tid == oc_data["ID"]).get()
    except DoesNotExist:
        return

    if faction.guild is None:
        return
    if str(faction.tid) in faction.guild.oc_config:
        OC_DELAY = faction.guild.oc_config[str(faction.tid)].get("delay", {"channel": 0, "roles": []}).get(
            "channel"
        ) not in [
            None,
            0,
        ]
        OC_READY = faction.guild.oc_config[str(faction.tid)].get("ready", {"channel": 0, "roles": []}).get(
            "channel"
        ) not in [
            None,
            0,
        ]
        OC_INITIATED = faction.guild.oc_config[str(faction.tid)].get("initiated", {"channel": 0}).get(
            "channel"
        ) not in [
            None,
            0,
        ]
    else:
        OC_DELAY = False
        OC_READY = False
        OC_INITIATED = False

    # OC ready/delay/init notifs
    for oc_id, oc_data in oc_data["crimes"].items():
        oc_db: OrganizedCrime = (
            OrganizedCrime.select()
            .join(User, JOIN.LEFT_OUTER, on=OrganizedCrime.initiated_by)
            .where(OrganizedCrime.oc_id == oc_id)
            .first()
        )

        OrganizedCrime.insert(
            faction_tid=faction.tid,
            oc_id=oc_id,
            crime_id=oc_data["crime_id"],
            participants=[int(list(participant.keys())[0]) for participant in oc_data["participants"]],
            time_started=None
            if oc_data["time_started"] == 0
            else datetime.datetime.fromtimestamp(oc_data["time_started"], tz=datetime.timezone.utc),
            time_ready=None
            if oc_data["time_ready"] == 0
            else datetime.datetime.fromtimestamp(oc_data["time_ready"], tz=datetime.timezone.utc),
            time_completed=None
            if oc_data["time_completed"] == 0
            else datetime.datetime.fromtimestamp(oc_data["time_completed"], tz=datetime.timezone.utc),
            planned_by=oc_data["planned_by"],
            initiated_by=oc_data["initiated_by"] if oc_data["initiated_by"] != 0 else None,
            money_gain=oc_data["money_gain"] if oc_data["money_gain"] != 0 else None,
            respect_gain=oc_data["respect_gain"] if oc_data["respect_gain"] != 0 else None,
            delayers=[],
        ).on_conflict(
            conflict_target=[OrganizedCrime.oc_id],
            preserve=[
                OrganizedCrime.faction_tid,
                OrganizedCrime.crime_id,
                OrganizedCrime.participants,
                OrganizedCrime.time_started,
                OrganizedCrime.time_completed,
                OrganizedCrime.planned_by,
                OrganizedCrime.initiated_by,
                OrganizedCrime.money_gain,
                OrganizedCrime.respect_gain,
                OrganizedCrime.delayers,
                OrganizedCrime.notified,
                OrganizedCrime.initiated,
            ],
        ).execute()

        if oc_db is None:
            continue
        elif oc_db.time_completed is not None:
            if (
                OC_INITIATED and time.time() - oc_db.time_completed.timestamp() <= 299
            ):  # Prevents old OCs from being notified
                if oc_db.money_gain == 0 and oc_db.respect_gain == 0:
                    oc_status_str = "unsuccessfully"
                    oc_result_str = ""
                    oc_color = SKYNET_ERROR
                else:
                    oc_status_str = "successfully"
                    oc_result_str = f" resulting in the gain of ${commas(oc_db.money_gain)} and {commas(oc_db.respect_gain)} respect"
                    oc_color = SKYNET_GOOD

                if oc_db.initiated_by is None or oc_db.initiated_by.name in (None, ""):
                    initiator_str = "Someone"
                else:
                    initiator_str = f"{oc_db.initiated_by.name} [{oc_db.initiated_by.tid}]"

                payload = {
                    "embeds": [
                        {
                            "title": f"OC of {faction.name} Initiated",
                            "description": f"{ORGANIZED_CRIMES[oc_data['crime_id']]} has been {oc_status_str} "
                            f"initiated by {initiator_str}{oc_result_str}.",
                            "color": oc_color,
                            "timestamp": datetime.datetime.utcnow().isoformat(),
                            "footer": {"text": f"#{oc_db.oc_id}"},
                        }
                    ],
                }

                try:
                    discordpost.delay(
                        f'channels/{faction.guild.oc_config[str(faction.tid)]["initiated"]["channel"]}/messages',
                        payload=payload,
                    )
                except Exception as e:
                    logger.exception(e)
                    continue

            continue
        elif oc_db.time_ready.timestamp() > int(time.time()):
            continue

        ready = list(
            map(
                lambda participant: list(participant.values())[0]["color"] == "green",
                oc_data["participants"],
            )
        )

        if OC_DELAY and len(oc_db.delayers) == 0 and not all(ready):
            # OC has been delayed
            oc_db.notified = False

            payload = {
                "embeds": [
                    {
                        "title": f"OC of {faction.name} Delayed",
                        "description": f"{ORGANIZED_CRIMES[oc_data['crime_id']]} has been delayed "
                        f"({ready.count(True)}/{len(oc_data['participants'])}).",
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": f"#{oc_db.oc_id}"},
                        "color": SKYNET_ERROR,
                    }
                ],
                "components": [],
            }

            roles = faction.guild.oc_config[str(faction.tid)]["delay"]["roles"]

            if len(roles) != 0:
                roles_str = ""

                for role in roles:
                    roles_str += f"<@&{role}>"

                payload["content"] = roles_str

            for participant in oc_data["participants"]:
                participant_id = list(participant.keys())[0]
                participant = participant[participant_id]

                if participant["color"] != "green":
                    oc_db.delayers.append(participant_id)

                    participant_db: typing.Optional[User] = (
                        User.select(User.discord_id).where(User.tid == participant_id).first()
                    )

                    if participant_db is not None and participant_db.discord_id not in ("", 0, None):
                        send_dm.delay(
                            discord_id=participant_db.discord_id,
                            payload={
                                "embeds": [
                                    {
                                        "title": "OC Delayed",
                                        "description": f"You are currently delaying the "
                                        f"{ORGANIZED_CRIMES[oc_data['crime_id']]} that you are participating in which "
                                        f"was ready <t:{oc_db.time_ready}:R>. Please return to Torn or otherwise "
                                        f"become available for the OC to be initiated.",
                                        "timestamp": datetime.datetime.utcnow().isoformat(),
                                        "footer": {"text": f"#{oc_db.oc_id}"},
                                        "color": SKYNET_ERROR,
                                    }
                                ]
                            },
                        ).forget()

                    if participant_db is None:
                        payload["components"].append(
                            {
                                "type": 1,
                                "components": [
                                    {
                                        "type": 2,
                                        "style": 5,
                                        "label": f"Unknown [{participant_id}]",
                                        "url": f"https://www.torn.com/profiles.php?XID={participant_id}",
                                    },
                                    {
                                        "type": 2,
                                        "style": 2,
                                        "label": f"{participant['description']}",
                                        "custom_id": f"participant:delay:{participant_id}",
                                        "disabled": True,
                                    },
                                ],
                            }
                        )
                    else:
                        payload["components"].append(
                            {
                                "type": 1,
                                "components": [
                                    {
                                        "type": 2,
                                        "style": 5,
                                        "label": f"{participant_db.name} [{participant_id}]",
                                        "url": f"https://www.torn.com/profiles.php?XID={participant_id}",
                                    },
                                    {
                                        "type": 2,
                                        "style": 2,
                                        "label": f"{participant['description']}",
                                        "custom_id": f"participant:delay:{participant_id}",
                                        "disabled": True,
                                    },
                                ],
                            }
                        )

            oc_db.save()

            try:
                discordpost.delay(
                    f'channels/{faction.guild.oc_config[str(faction.tid)]["delay"]["channel"]}/messages',
                    payload=payload,
                ).forget()
            except Exception as e:
                logger.exception(e)
                continue
        elif OC_READY and not oc_db.notified and all(ready):
            # OC is ready
            oc_db.notified = True
            oc_db.save()

            payload = {
                "embeds": [
                    {
                        "title": f"OC of {faction.name} Ready",
                        "description": f"{ORGANIZED_CRIMES[oc_data['crime_id']]} is ready.",
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "footer": {"text": f"#{oc_db.oc_id}"},
                        "color": SKYNET_GOOD,
                    }
                ],
            }

            roles = faction.guild.oc_config[str(faction.tid)]["ready"]["roles"]

            if len(roles) != 0:
                roles_str = ""

                for role in roles:
                    roles_str += f"<@&{role}>"

                payload["content"] = roles_str

            try:
                discordpost.delay(
                    f'channels/{faction.guild.oc_config[str(faction.tid)]["ready"]["channel"]}/messages',
                    payload=payload,
                )
            except Exception as e:
                logger.exception(e)
                continue


@celery.shared_task(
    name="tasks.faction.auto_cancel_requests",
    routing_key="default.auto_cancel_requests",
    queue="default",
)
def auto_cancel_requests():
    withdrawal: Withdrawal
    for withdrawal in Withdrawal.select().where(
        (Withdrawal.status == 0)
        & (Withdrawal.time_requested <= datetime.datetime.utcnow() - datetime.timedelta(hours=1))
    ):  # Two hours before now
        withdrawal.status = 3
        withdrawal.time_fulfilled = datetime.datetime.utcnow()
        withdrawal.save()

        requester: typing.Optional[User] = User.select(User.discord_id).where(User.tid == withdrawal.requester).first()

        if requester is None or requester.discord_id in (0, None):
            continue

        try:
            faction: Faction = Faction.select(Faction.guild).join(Server).where(Faction.tid == withdrawal.faction_tid)
        except DoesNotExist:
            continue

        try:
            if faction.guild is not None and str(faction.tid) in faction.guild.banking_config:
                discordpatch.delay(
                    f"channels/{faction.guild.banking_config[str(faction.tid)]['channel']}/messages/{withdrawal.withdrawal_message}",
                    {
                        "embeds": [
                            {
                                "title": f"Vault Request #{withdrawal.wid}",
                                "description": "This request has timed-out and been automatically cancelled by the "
                                "system.",
                                "fields": [
                                    {
                                        "name": "Original Request Amount",
                                        "value": f"{commas(withdrawal.amount)} {'Cash' if withdrawal.cash_request else 'Points'}",
                                    },
                                    {
                                        "name": "Original Requester",
                                        "value": f"{requester.name} [{requester.tid}]",
                                    },
                                ],
                                "timestamp": datetime.datetime.utcnow().isoformat(),
                                "color": SKYNET_ERROR,
                            }
                        ],
                        "components": [
                            {
                                "type": 1,
                                "components": [
                                    {
                                        "type": 2,
                                        "style": 5,
                                        "label": "Faction Vault",
                                        "url": "https://www.torn.com/factions.php?step=your#/tab=controls&option="
                                        "give-to-user",
                                        "disabled": True,
                                    },
                                    {
                                        "type": 2,
                                        "style": 5,
                                        "label": "Fulfill",
                                        "url": f"https://tornium.com/faction/banking/fulfill/{withdrawal.guid}",
                                        "disabled": True,
                                    },
                                    {
                                        "type": 2,
                                        "style": 3,
                                        "label": "Fulfill Manually",
                                        "custom_id": "faction:vault:fulfill",
                                        "disabled": True,
                                    },
                                    {
                                        "type": 2,
                                        "style": 4,
                                        "label": "Cancel",
                                        "custom_id": "faction:vault:cancel",
                                        "disabled": True,
                                    },
                                ],
                            }
                        ],
                    },
                )

        except Exception as e:
            logger.exception(e)

        try:
            dm_channel = discordpost("users/@me/channels", payload={"recipient_id": requester.discord_id})
        except (DiscordError, NetworkingError):
            continue
        except Exception as e:
            logger.exception(e)
            continue

        discordpost.delay(
            f"channels/{dm_channel['id']}/messages",
            payload={
                "embeds": [
                    {
                        "title": "Vault Request Cancelled",
                        "description": f"Your vault request #{withdrawal.wid} has timed-out and has been automatically "
                        f"cancelled. Vault requests will be automatically cancelled after about an hour. If "
                        f"you still require this, please submit a new request.",
                        "timestamp": datetime.datetime.utcnow().isoformat(),
                        "color": SKYNET_ERROR,
                    }
                ]
            },
        ).forget()


@celery.shared_task(name="tasks.faction.armory_check", routing_key="quick.armory_check", queue="quick")
def armory_check():
    faction: Faction
    for faction in (
        Faction.select(Faction.guild, Faction.tid, Faction.aa_keys).join(Server).where(Faction.aa_keys != [])
    ):
        if len(faction.aa_keys) == 0:
            continue
        elif faction.guild is None:
            continue

        if faction.tid not in faction.guild.factions:
            continue
        elif not faction.guild.armory_enabled:
            continue
        elif str(faction.tid) not in faction.guild.armory_config:
            continue
        elif not faction.guild.armory_config[str(faction.tid)].get("enabled", False):
            continue
        elif faction.guild.armory_config[str(faction.tid)].get("channel", 0) == 0:
            continue
        elif len(faction.guild.armory_config[str(faction.tid)].get("items", {})) == 0:
            continue

        aa_key = random.choice(faction.aa_keys)

        tornget.signature(
            kwargs={
                "endpoint": "faction/?selections=armor,boosters,drugs,medical,temporary,weapons",
                "key": aa_key,
            },
            queue="api",
        ).apply_async(
            expires=300,
            link=armory_check_subtask.signature(
                kwargs={
                    "faction_id": faction.tid,
                },
                queue="quick",
            ),
        )


@celery.shared_task(
    name="tasks.faction.armory_check_subtask",
    routing_key="quick.armory_check_subtask",
    queue="quick",
)
def armory_check_subtask(_armory_data, faction_id: int):
    try:
        faction: Faction = Faction.select().where(Faction.tid == faction_id).get()
    except DoesNotExist:
        return

    if faction.guild is None:
        return
    elif faction.tid not in faction.guild.factions:
        return
    elif not faction.guild.armory_enabled:
        return
    elif str(faction.tid) not in faction.guild.armory_config:
        return

    faction_config = faction.guild.armory_config[str(faction.tid)]

    if not faction_config.get("enabled", False):
        return
    elif faction_config.get("channel", 0) == 0:
        return
    elif len(faction_config.get("items", {})) == 0:
        return

    role_str = "".join([f"<@&{role}>" for role in faction_config.get("roles", [])])

    for armory_type in _armory_data:
        for armory_item in _armory_data[armory_type]:
            if str(armory_item["ID"]) not in faction_config["items"]:
                continue

            quantity = armory_item.get("available") or armory_item.get("quantity")
            minimum = faction_config["items"][str(armory_item["ID"])]

            if quantity >= minimum:
                continue

            item: typing.Optional[Item] = Item.select(Item.market_value).where(Item.tid == armory_item["ID"]).first()

            if item is None or item.market_value <= 0:
                suffix = ""
            else:
                suffix = f" (worth about ${commas(item.market_value * (minimum - quantity))})"

            payload = {
                "embeds": [
                    {
                        "title": "Low Armory Stock",
                        "description": f"{faction.name} is currently low on {armory_item['name']} ({commas(quantity)} "
                        f"remaining). {commas(minimum - quantity)}x must be bought to meet the minimum quantity{suffix}.",
                        "color": SKYNET_ERROR,
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
                                "style": 5,
                                "label": "Armory",
                                "url": "https://www.torn.com/factions.php?step=your&type=1#/tab=armoury",
                            },
                            {
                                "type": 2,
                                "style": 5,
                                "label": "Item Market",
                                "url": f"https://www.torn.com/imarket.php#/p=shop&step=shop&type=&searchname={armory_item['ID']}",
                            },
                        ],
                    },
                ],
            }

            if role_str != "":
                payload["content"] = role_str

            try:
                discordpost.delay(
                    f"channels/{faction_config['channel']}/messages",
                    payload=payload,
                    channel=faction_config["channel"],
                ).forget()
            except Exception as e:
                logger.exception(e)
                continue

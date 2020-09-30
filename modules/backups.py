import xenon_worker as wkr
import asyncio
import pymongo
from pymongo import errors as mongoerrors
from datetime import datetime, timedelta
import random
import msgpack

import utils
import checks
from backups import BackupSaver, BackupLoader

MAX_BACKUPS = 15


class BackupListMenu(wkr.ListMenu):
    embed_kwargs = {"title": "Your Backups"}

    async def get_items(self):
        args = {
            "limit": 10,
            "skip": self.page * 10,
            "sort": [("timestamp", pymongo.DESCENDING)],
            "filter": {
                "creator": self.ctx.author.id,
            }
        }
        backups = self.ctx.bot.db.backups.find(**args)
        items = []
        async for backup in backups:
            items.append((
                backup["_id"] + (" ⏲️" if backup.get("interval") else ""),
                f"{backup['data']['name']} (`{utils.datetime_to_string(backup['timestamp'])} UTC`)"
            ))

        return items


class Backups(wkr.Module):
    @wkr.Module.listener()
    async def on_load(self, *_, **__):
        await self.bot.db.backups.create_index([("creator", pymongo.ASCENDING)])
        await self.bot.db.backups.create_index([("timestamp", pymongo.ASCENDING)])
        await self.bot.db.backups.create_index([("data.id", pymongo.ASCENDING)])
        await self.bot.db.intervals.create_index([("guild", pymongo.ASCENDING), ("user", pymongo.ASCENDING)])
        await self.bot.db.intervals.create_index([("next", pymongo.ASCENDING)])
        await self.bot.db.id_translators.create_index(
            [("source_id", pymongo.ASCENDING), ("target_id", pymongo.ASCENDING)],
            unique=True
        )

    @wkr.Module.command(aliases=("backups", "bu"))
    async def backup(self, ctx):
        """
        Create & load private backups of your servers
        """
        await ctx.invoke("help backup")

    @backup.command(hidden=True)
    @checks.is_staff(level=checks.StaffLevel.ADMIN)
    async def transfer(self, ctx, backup_id, user: wkr.UserConverter):
        """
        Transfer a backup to the specified user
        """
        user = await user(ctx)
        res = await ctx.bot.db.backups.update_one({"_id": backup_id}, {"$set": {"creator": str(user.id)}})
        if res.matched_count == 0:
            raise ctx.f.ERROR(f"There is **no backup** with the id `{backup_id}`.")

        raise ctx.f.SUCCESS(f"Successfully transferred backup.")

    @backup.command(aliases=("c",))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @wkr.cooldown(1, 10, bucket=wkr.CooldownType.GUILD)
    async def create(self, ctx):
        """
        Create a backup
        
        Get more help on the [wiki](https://wiki.xenon.bot/backups#creating-a-backup).


        __Examples__

        ```{b.prefix}backup create```
        """
        backup_count = await ctx.bot.db.backups.count_documents({"creator": ctx.author.id})
        if backup_count >= MAX_BACKUPS:
            raise ctx.f.ERROR(
                f"You have **exceeded the maximum count** of backups. (`{backup_count}/{MAX_BACKUPS}`)\n"
                f"You need to **delete old backups** with `{ctx.bot.prefix}backup delete <id>` or **buy "
                f"[Xenon Premium](https://www.patreon.com/merlinfuchs)** to create new backups.."
            )

        status_msg = await ctx.f_send("**Creating Backup** ...", f=ctx.f.WORKING)
        guild = await ctx.get_full_guild()
        backup = BackupSaver(ctx.client, guild)
        await backup.save()

        backup_id = utils.unique_id()
        try:
            await ctx.bot.db.backups.insert_one({
                "_id": backup_id,
                "creator": ctx.author.id,
                "timestamp": datetime.utcnow(),
                "data": backup.data
            })
        except mongoerrors.DocumentTooLarge:
            raise ctx.f.ERROR(
                f"This backups **exceeds** the maximum size of **16 Megabyte**. Your server probably has a lot of "
                f"members and channels containing messages. Try to create a new backup with less messages (chatlog)."
            )

        embed = ctx.f.format(f"Successfully **created backup** with the id `{backup_id}`.", f=ctx.f.SUCCESS)["embed"]
        embed.setdefault("fields", []).append({
            "name": "Usage",
            "value": f"```{ctx.bot.prefix}backup load {backup_id}```\n"
                     f"```{ctx.bot.prefix}backup info {backup_id}```"
        })
        await ctx.client.edit_message(status_msg, embed=embed)
        await ctx.bot.create_audit_log(utils.AuditLogType.BACKUP_CREATE, [ctx.guild_id], ctx.author.id)

    @backup.command(aliases=("l",))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @wkr.cooldown(1, 60, bucket=wkr.CooldownType.GUILD)
    async def load(self, ctx, backup_id, *options):
        """
        Load a backup
        
        Get more help on the [wiki](https://wiki.xenon.bot/backups#loading-a-backup).


        __Arguments__

        **backup_id**: The id of the backup
        **options**: A list of options (See examples)


        __Examples__

        Default options: ```{b.prefix}backup load oj1xky11871fzrbu```
        Only roles: ```{b.prefix}backup load oj1xky11871fzrbu !* roles```
        Everything but bans: ```{b.prefix}backup load oj1xky11871fzrbu !bans```
        """
        backup_d = await ctx.client.db.backups.find_one({"_id": backup_id, "creator": ctx.author.id})
        if backup_d is None:
            raise ctx.f.ERROR(f"You have **no backup** with the id `{backup_id}`.")

        warning_msg = await ctx.f_send("Are you sure that you want to load this backup?\n"
                                       f"Please put the managed role called `{ctx.bot.user.name}` above all other "
                                       f"roles before clicking the ✅ reaction.\n\n"
                                       "__**All channels and roles will get replaced!**__\n\n"
                                       "*Also keep in mind that you can only load up to 250 roles per 48 hours.*",
                                       f=ctx.f.WARNING)
        reactions = ("✅", "❌")
        for reaction in reactions:
            await ctx.client.add_reaction(warning_msg, reaction)

        try:
            data, = await ctx.client.wait_for(
                "message_reaction_add",
                ctx.shard_id,
                check=lambda d: d["message_id"] == warning_msg.id and
                                d["user_id"] == ctx.author.id and
                                d["emoji"]["name"] in reactions,
                timeout=60
            )
        except asyncio.TimeoutError:
            await ctx.client.delete_message(warning_msg)
            return

        await ctx.client.delete_message(warning_msg)
        if data["emoji"]["name"] != "✅":
            return

        guild = await ctx.get_full_guild()
        backup = BackupLoader(ctx.client, guild, backup_d["data"], reason="Backup loaded by " + str(ctx.author))

        await self.client.redis.publish("loaders:start", msgpack.packb({
            "id": ctx.guild_id,
            "type": "backup",
            "source_id": backup.data["id"],
            "backup_id": backup_id
        }))
        try:
            await backup.load(**utils.backup_options(options))

            unpacked_ids = {
                f"ids.{s}": t
                for s, t in backup.id_translator.items()
            }
            await ctx.bot.db.id_translators.update_one(
                {
                    "target_id": ctx.guild_id,
                    "source_id": backup.data["id"],
                },
                {
                    "$set": {
                        "target_id": ctx.guild_id,
                        "source_id": backup.data["id"],
                        **unpacked_ids
                    },
                    "$addToSet": {
                        "loaders": ctx.author.id
                    }
                },
                upsert=True
            )
        finally:
            await self.client.redis.publish("loaders:done", msgpack.packb({
                "id": ctx.guild_id,
                "type": "backup",
                "source_id": backup.data["id"],
                "backup_id": backup_id
            }))
            await ctx.bot.create_audit_log(utils.AuditLogType.BACKUP_LOAD, [ctx.guild_id], ctx.author.id)

    @backup.command(aliases=("del", "remove", "rm"))
    @wkr.cooldown(5, 30)
    async def delete(self, ctx, backup_id):
        """
        Delete one of your backups
        
        Get more help on the [wiki](https://wiki.xenon.bot/backups#deleting-a-backup).
        __**This cannot be undone**__


        __Examples__

        ```{b.prefix}backup delete 3zpssue46g```
        """
        result = await ctx.client.db.backups.delete_one({"_id": backup_id, "creator": ctx.author.id})
        if result.deleted_count > 0:
            raise ctx.f.SUCCESS("Successfully **deleted backup**.")

        else:
            raise ctx.f.ERROR(f"You have **no backup** with the id `{backup_id}`.")

    @backup.command(aliases=("clear",))
    @wkr.cooldown(1, 60, bucket=wkr.CooldownType.GUILD)
    async def purge(self, ctx, before=None):
        """
        Delete all your backups
        __**This cannot be undone**__


        __Arguments__

        **limit**: Either the number of backups to delete (starting with the oldest) or a date (YYYY-MM-DD)
        Using a date will delete all backups created __before__ the date.


        __Examples__

        ```{b.prefix}backup purge```
        """
        filter = {"creator": ctx.author.id}
        if before is not None:
            try:
                filter["timestamp"] = {
                    "$lte": datetime.strptime(
                        before.replace(":", "").replace("-", "").replace("/", ""),
                        "%Y%m%d"
                    )
                }
            except ValueError:
                raise ctx.f.ERROR(f"The value `{before}` is **not a valid date**. "
                                  f"Please format it like `YYYY-MM-DD`, including padding zeroes.")

        warning_msg = await ctx.f_send("Are you sure that you want to delete all (or some) of your backups?\n"
                                       "__**This cannot be undone!**__", f=ctx.f.WARNING)
        reactions = ("✅", "❌")
        for reaction in reactions:
            await ctx.client.add_reaction(warning_msg, reaction)

        try:
            data, = await ctx.client.wait_for(
                "message_reaction_add",
                ctx.shard_id,
                check=lambda d: d["message_id"] == warning_msg.id and
                                d["user_id"] == ctx.author.id and
                                d["emoji"]["name"] in reactions,
                timeout=60
            )
        except asyncio.TimeoutError:
            await ctx.client.delete_message(warning_msg)
            return

        await ctx.client.delete_message(warning_msg)
        if data["emoji"]["name"] != "✅":
            return

        await ctx.client.db.backups.delete_many(filter)
        raise ctx.f.SUCCESS("Successfully **deleted all your backups**.")

    @backup.command(aliases=("ls",))
    @wkr.cooldown(1, 10)
    async def list(self, ctx):
        """
        Get a list of your backups


        __Examples__

        ```{b.prefix}backup list```
        """
        menu = BackupListMenu(ctx)
        await menu.start()

    @backup.command(aliases=("i",))
    @wkr.cooldown(5, 30)
    async def info(self, ctx, backup_id):
        """
        Get information about a backup


        __Arguments__

        **backup_id**: The id of the backup


        __Examples__

        ```{b.prefix}backup info 3zpssue46g```
        """
        backup = await ctx.client.db.backups.find_one({"_id": backup_id, "creator": ctx.author.id})
        if backup is None:
            raise ctx.f.ERROR(f"You have **no backup** with the id `{backup_id}`.")

        backup["data"].pop("members", None)
        guild = wkr.Guild(backup["data"])

        channels = utils.channel_tree(guild.channels)
        if len(channels) > 1024:
            channels = channels[:1000] + "\n...\n```"

        roles = "```{}```".format("\n".join([
            r.name for r in sorted(guild.roles, key=lambda r: r.position, reverse=True)
        ]))
        if len(roles) > 1024:
            roles = roles[:1000] + "\n...\n```"

        raise ctx.f.DEFAULT(embed={
            "title": guild.name,
            "fields": [
                {
                    "name": "Created At",
                    "value": utils.datetime_to_string(backup["timestamp"]) + " UTC",
                    "inline": False
                },
                {
                    "name": "Channels",
                    "value": channels,
                    "inline": True
                },
                {
                    "name": "Roles",
                    "value": roles,
                    "inline": True
                }
            ]
        })

    @backup.command(aliases=("iv",))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @wkr.cooldown(1, 10, bucket=wkr.CooldownType.GUILD)
    async def interval(self, ctx, *interval):
        """
        Manage automated backups
        
        Get more help on the [wiki](https://wiki.xenon.bot/en/backups#automated-backups-interval).


        __Arguments__

        **interval**: The time between every backup or "off". (min 24h)
                    Supported units: hours(h), days(d), weeks(w)
                    Example: 1d 12h


        __Examples__

        ```{b.prefix}backup interval 24h```
        """
        if len(interval) > 0:
            await ctx.invoke("backup interval on " + " ".join(interval))
            return

        interval = await ctx.bot.db.intervals.find_one({"guild": ctx.guild_id, "user": ctx.author.id})
        if interval is None:
            raise ctx.f.INFO("The **backup interval is** currently turned **off**.\n"
                             f"Turn it on with `{ctx.bot.prefix}backup interval on 24h`.")

        else:
            raise ctx.f.INFO(embed={
                "author": {
                    "name": "Backup Interval"
                },
                "fields": [
                    {
                        "name": "Interval",
                        "value": utils.timedelta_to_string(timedelta(hours=interval["interval"])),
                        "inline": True
                    },
                    {
                        "name": "Last Backup",
                        "value": utils.datetime_to_string(interval["last"]),
                        "inline": False
                    },
                    {
                        "name": "Next Backup",
                        "value": utils.datetime_to_string(interval["next"]),
                        "inline": False
                    }
                ]
            })

    @interval.command(aliases=["enable"])
    @wkr.cooldown(1, 10, bucket=wkr.CooldownType.GUILD)
    async def on(self, ctx, *interval):
        """
        Turn on automated backups


        __Arguments__

        **interval**: The time between every backup. (min 24h)
                    Supported units: hours(h), days(d), weeks(w)
                    Example: 1d 12h


        __Examples__

        ```{b.prefix}backup interval on 24h```
        """
        units = {
            "h": 1,
            "d": 24,
            "w": 24 * 7
        }

        hours = 0
        for arg in interval:
            try:
                count, unit = int(arg[:-1]), arg[-1]
            except (ValueError, IndexError):
                continue

            multiplier = units.get(unit.lower(), 1)
            hours += count * multiplier

        hours = max(hours, 24)

        now = datetime.utcnow()
        td = timedelta(hours=hours)
        await ctx.bot.db.intervals.update_one({"guild": ctx.guild_id, "user": ctx.author.id}, {"$set": {
            "guild": ctx.guild_id,
            "user": ctx.author.id,
            "last": now,
            "next": now,
            "interval": hours
        }}, upsert=True)

        await ctx.bot.create_audit_log(utils.AuditLogType.BACKUP_INTERVAL_ENABLE, [ctx.guild_id], ctx.author.id)
        raise ctx.f.SUCCESS("Successful **enabled the backup interval**.\nThe first backup will be created in "
                            f"`{utils.timedelta_to_string(td)}` "
                            f"at `{utils.datetime_to_string(datetime.utcnow() + td)} UTC`.")

    @interval.command(aliases=["disable"])
    @wkr.cooldown(1, 10, bucket=wkr.CooldownType.GUILD)
    async def off(self, ctx):
        """
        Turn off automated backups


        __Examples__

        ```{b.prefix}backup interval off```
        """
        result = await ctx.bot.db.intervals.delete_one({"guild": ctx.guild_id, "user": ctx.author.id})
        if result.deleted_count > 0:
            await ctx.bot.create_audit_log(utils.AuditLogType.BACKUP_INTERVAL_DISABLE, [ctx.guild_id], ctx.author.id)
            raise ctx.f.SUCCESS("Successfully **disabled the backup interval**.")

        else:
            raise ctx.f.ERROR(f"The backup interval is not enabled.")

    @wkr.Module.task(minutes=random.randint(5, 15))
    async def interval_task(self):
        async def _run_interval_backups(interval):
            guild = await self.bot.get_full_guild(interval["guild"])
            if guild is None:
                return

            backup = BackupSaver(self.bot, guild)
            await backup.save()

            await self.bot.db.backups.delete_one({"creator": interval["user"], "data.id": guild.id})
            await self.bot.db.backups.insert_one({
                "_id": utils.unique_id(),
                "creator": interval["user"],
                "timestamp": datetime.utcnow(),
                "interval": True,
                "data": backup.data
            })

        to_backup = self.bot.db.intervals.find({"next": {"$lt": datetime.utcnow()}})
        async for interval in to_backup:
            self.bot.schedule(_run_interval_backups(interval))
            await self.bot.db.intervals.update_one({"_id": interval["_id"]}, {"$set": {
                "next": interval["next"] + timedelta(hours=interval["interval"]),
                "last": datetime.utcnow()
            }})

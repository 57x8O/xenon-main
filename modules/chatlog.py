import xenon_worker as wkr
from datetime import datetime, timedelta
import pymongo
import asyncio
import io

import checks
import utils


class ChatlogListMenu(wkr.ListMenu):
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
        chatlogs = self.ctx.bot.db.premium.chatlogs.find(**args)
        items = []
        async for chatlog in chatlogs:
            items.append((
                chatlog["_id"],
                f"<#{chatlog['channel']}> (`{utils.datetime_to_string(chatlog['timestamp'])} UTC`)"
            ))

        return items


class Chatlog(wkr.Module):
    @wkr.Module.listener()
    async def on_load(self, *_, **__):
        await self.bot.db.premium.chatlogs.create_index([("creator", pymongo.ASCENDING)])
        await self.bot.db.premium.chatlogs.create_index([("timestamp", pymongo.ASCENDING)])

    @wkr.Module.task(hours=24)
    async def message_retention(self):
        await self.bot.db.delete_many({
            "msg_retention": True,
            "timestamp": {
                "$lte": datetime.utcnow() - timedelta(days=30)
            }
        })

    @wkr.Module.command(aliases=("chatlogs", "cl"))
    async def chatlog(self, ctx):
        """
        Save & load messages from individual channels
        """
        await ctx.invoke("help chatlog")

    async def _create_chatlog(self, channel_id, count):
        return [
            {
                "id": message.id,
                "content": message.content,
                "author": message.author.user.to_dict(),
                "attachments": [
                    {
                        "filename": attachment["filename"],
                        "url": attachment["url"]
                    }
                    for attachment in message.attachments
                ],
                "pinned": message.pinned,
                "embeds": message.embeds
            }
            async for message in self.client.iter_messages(wkr.Snowflake(channel_id), count)
        ]

    async def _load_chatlog(self, data, channel_id, count):
        webhook = await self.client.create_webhook(wkr.Snowflake(channel_id), name="backup")
        for msg in reversed(data[:count]):
            author = wkr.User(msg["author"])

            attachments = msg.get("attachments", [])
            files = []

            async def _fetch_attachment(attachment):
                async with self.bot.session.get(attachment["url"]) as resp:
                    if resp.status == 200:
                        fp = io.BytesIO(await resp.read())
                        files.append(wkr.File(fp, filename=attachment["filename"]))

            file_tasks = [self.bot.schedule(_fetch_attachment(att)) for att in attachments]
            if file_tasks:
                await asyncio.wait(file_tasks, return_when=asyncio.ALL_COMPLETED)

            try:
                await self.client.execute_webhook(
                    webhook,
                    wait=True,
                    username=author.name,
                    avatar_url=author.avatar_url,
                    allowed_mentions={"parse": []},
                    files=files,
                    **msg
                )
            except wkr.NotFound:
                break

            except Exception:
                pass

        await self.client.delete_webhook(webhook)

    @chatlog.command(aliases=("c",))
    @wkr.guild_only
    @checks.has_permissions_level()
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    @wkr.cooldown(1, 10, bucket=wkr.CooldownType.GUILD)
    async def create(self, ctx, count: int = 100):
        """
        Save the last <count> messages in this channel


        __Examples__

        Save 100 messages: ```{b.prefix}chatlog create 100```
        """
        max_chatlogs = 25
        if ctx.premium == checks.PremiumLevel.ONE:
            count = min(count, 250)

        elif ctx.premium == checks.PremiumLevel.TWO:
            count = min(count, 500)
            max_chatlogs = 50

        elif ctx.premium == checks.PremiumLevel.THREE:
            count = min(count, 1000)
            max_chatlogs = 100

        chatlog_count = await ctx.bot.db.premium.chatlogs.count_documents({"creator": ctx.author.id})
        if chatlog_count >= max_chatlogs:
            raise ctx.f.ERROR(
                f"You have **exceeded the maximum count** of chatlog. (`{chatlog_count}/{max_chatlogs}`)\n"
                f"You need to **delete old chatlogs** with `{ctx.bot.prefix}chatlog delete <id>` or **buy "
                f"[Xenon Premium](https://www.patreon.com/merlinfuchs)** to create new chatlog.."
            )

        status_msg = await ctx.f_send("**Creating Chatlog** ...", f=ctx.f.WORKING)
        data = await self._create_chatlog(ctx.channel_id, count)
        chatlog_id = utils.unique_id()
        await ctx.bot.db.premium.chatlogs.insert_one({
            "_id": chatlog_id,
            "msg_retention": True,
            "creator": ctx.author.id,
            "timestamp": datetime.utcnow(),
            "channel": ctx.channel_id,
            "data": data
        })

        embed = ctx.f.format(f"Successfully **created chatlog** with the id `{chatlog_id}`.", f=ctx.f.SUCCESS)["embed"]
        embed.setdefault("fields", []).append({
            "name": "Usage",
            "value": f"```{ctx.bot.prefix}chatlog load {chatlog_id}```\n"
                     f"```{ctx.bot.prefix}chatlog info {chatlog_id}```"
        })
        await ctx.client.edit_message(status_msg, embed=embed)
        await ctx.bot.create_audit_log(
            utils.AuditLogType.CHATLOG_CREATE, [ctx.guild_id], ctx.author.id, {"channel": ctx.channel_id}
        )

    @chatlog.command(aliases=("l",))
    @wkr.guild_only
    @checks.has_permissions_level(destructive=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    @wkr.cooldown(1, 10, bucket=wkr.CooldownType.GUILD)
    async def load(self, ctx, chatlog_id, count: int = 1000):
        """
        Load messages from a chatlog


        __Arguments__

        **chatlog_id**: The id of the chatlog
        **count**: The count of messages you want to load


        __Examples__

        Load 100 messages: ```{b.prefix}chatlog load oj1xky11871fzrbu 100```
        """
        chatlog_d = await ctx.client.db.premium.chatlogs.find_one({"_id": chatlog_id, "creator": ctx.author.id})
        if chatlog_d is None:
            raise ctx.f.ERROR(f"You have **no chatlog** with the id `{chatlog_id}`.")

        warning_msg = await ctx.f_send("Are you sure that you want to load this chatlog?\n"
                                       "This can take multiple minutes!", f=ctx.f.WARNING)
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

        try:
            await self._load_chatlog(chatlog_d["data"], ctx.channel_id, count)
        finally:
            await ctx.bot.create_audit_log(
                utils.AuditLogType.CHATLOG_LOAD, [ctx.guild_id], ctx.author.id, {"channel": ctx.channel_id}
            )

    @chatlog.command(aliases=("del", "remove", "rm"))
    @wkr.cooldown(5, 30)
    async def delete(self, ctx, chatlog_id):
        """
        Delete one of your chatlogs
        __**This cannot be undone**__


        __Examples__

        ```{b.prefix}chatlog delete 3zpssue46g```
        """
        result = await ctx.client.db.premium.chatlogs.delete_one({"_id": chatlog_id, "creator": ctx.author.id})
        if result.deleted_count > 0:
            raise ctx.f.SUCCESS("Successfully **deleted chatlog**.")

        else:
            raise ctx.f.ERROR(f"You have **no chatlog** with the id `{chatlog_id}`.")

    @chatlog.command()
    @wkr.cooldown(1, 60, bucket=wkr.CooldownType.GUILD)
    async def purge(self, ctx):
        """
        Delete all your chatlogs
        __**This cannot be undone**__


        __Examples__

        ```{b.prefix}chatlog purge```
        """
        warning_msg = await ctx.f_send("Are you sure that you want to delete all your chatlogs?\n"
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

        await ctx.client.db.premium.chatlogs.delete_many({"creator": ctx.author.id})
        raise ctx.f.SUCCESS("Successfully **deleted all your chatlogs**.")

    @chatlog.command(aliases=("ls",))
    @wkr.cooldown(1, 10)
    async def list(self, ctx):
        """
        Get a list of your chatlogs


        __Examples__

        ```{b.prefix}chatlog list```
        """
        menu = ChatlogListMenu(ctx)
        await menu.start()

    @chatlog.command(aliases=("i",))
    @wkr.cooldown(5, 30)
    async def info(self, ctx, chatlog_id):
        """
        Get information about a chatlog


        __Arguments__

        **chatlog_id**: The id of the chatlog


        __Examples__

        ```{b.prefix}chatlog info 3zpssue46g```
        """
        chatlog = await ctx.client.db.premium.chatlogs.find_one({"_id": chatlog_id, "creator": ctx.author.id})
        if chatlog is None:
            raise ctx.f.ERROR(f"You have **no chatlog** with the id `{chatlog_id}`.")

        first_msg = wkr.Snowflake(chatlog["data"][-1]["id"])
        last_msg = wkr.Snowflake(chatlog["data"][0]["id"])

        raise ctx.f.DEFAULT(embed={
            "description": f"**Chatlog of <#{chatlog['channel']}>**",
            "fields": [
                {
                    "name": "Created At",
                    "value": utils.datetime_to_string(chatlog["timestamp"]) + " UTC",
                    "inline": False
                },
                {
                    "name": "Message Count",
                    "value": len(chatlog["data"]),
                    "inline": True
                },
                {
                    "name": "Time Range",
                    "value": f"`{utils.datetime_to_string(first_msg.created_at)} UTC`- "
                             f"`{utils.datetime_to_string(last_msg.created_at)} UTC`",
                    "inline": True
                }
            ]
        })

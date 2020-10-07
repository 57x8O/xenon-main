import xenon_worker as wkr
import pymongo
import pymongo.errors
from enum import Enum, IntEnum
import traceback
import asyncio
import io
import msgpack

import checks
import utils


class SyncDirection(Enum):
    FROM = 0
    TO = 1
    BOTH = 2


class SyncType(IntEnum):
    MESSAGES = 0
    BANS = 1
    ROLE = 2


class SyncListMenu(wkr.ListMenu):
    embed_kwargs = {"title": "Sync List"}

    async def get_items(self):
        args = {
            "limit": 10,
            "skip": self.page * 10,
            "filter": {
                "guilds": self.ctx.guild_id,
            }
        }
        syncs = self.ctx.bot.db.premium.syncs.find(**args)
        items = []
        async for sync in syncs:
            if sync["type"] == SyncType.MESSAGES:
                items.append((
                    sync["_id"],
                    f"Messages from <#{sync['source']}> to <#{sync['target']}>\n"
                    f"(`{sync.get('uses', 0)}` message(s) transferred)"
                ))

            elif sync["type"] == SyncType.BANS:
                items.append((
                    sync["_id"],
                    f"Bans from `{sync['source']}` to `{sync['target']}`\n"
                    f"(`{sync.get('uses', 0)}` ban(s) transferred)"
                ))

        return items


class Sync(wkr.Module):
    @wkr.Module.listener()
    async def on_load(self, *_, **__):
        await self.bot.db.premium.syncs.create_index(
            [("type", pymongo.ASCENDING), ("target", pymongo.ASCENDING), ("source", pymongo.ASCENDING)],
            unique=True
        )
        await self.bot.db.premium.syncs.create_index(
            [("guilds", pymongo.ASCENDING)]
        )

        await self.bot.subscribe("*.message_create", shared=True)

        await self.bot.subscribe("*.guild_ban_add", shared=True)
        await self.bot.subscribe("*.guild_ban_remove", shared=True)

        await self.bot.subscribe("*.guild_member_add", shared=True)
        await self.bot.subscribe("*.guild_member_update", shared=True)
        await self.bot.subscribe("*.guild_member_remove", shared=True)

        await self.bot.subscribe("*.channel_delete", shared=True)
        await self.bot.subscribe("*.guild_delete", shared=True)
        await self.bot.subscribe("*.guild_role_delete", shared=True)

    @wkr.Module.command()
    async def sync(self, ctx):
        """
        Sync messages and bans between different servers and channels
        """
        await ctx.invoke("help sync")

    @sync.command(aliases=("ls",))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    async def list(self, ctx):
        """
        Get a list of syncs associated with this guild


        __Examples__

        ```{b.prefix}backup list```
        """
        menu = SyncListMenu(ctx)
        return await menu.start()

    @sync.command(aliases=("del", "remove", "rm"))
    @wkr.guild_only
    @checks.has_permissions_level(destructive=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    async def delete(self, ctx, sync_id):
        """
        Delete a sync associated with this guild


        __Examples__

        ```{b.prefix}sync delete 3zpssue46g```
        """
        sync = await ctx.bot.db.premium.syncs.find_one_and_delete({"_id": sync_id, "guilds": ctx.guild_id})
        if sync is not None:
            await ctx.bot.create_audit_log(
                utils.AuditLogType.SYNC_DELETE, sync["guilds"], ctx.author.id,
                {"id": sync_id}
            )
            raise ctx.f.SUCCESS("Successfully **deleted sync**.")

        else:
            raise ctx.f.ERROR(f"There is **no sync** with the id `{sync_id}`.")

    async def _check_admin_on(self, guild, ctx):
        try:
            invoker = await self.client.fetch_member(guild, ctx.author.id)
        except wkr.NotFound:
            raise ctx.f.ERROR("You **need to be member** of the target guild.")

        perms = invoker.permissions_for_guild(guild)
        if not perms.administrator:
            raise ctx.f.ERROR("You **need to have `administrator`** in the target guild.")

        bot = await self.client.get_bot_member(guild.id)
        if bot is None:
            raise ctx.f.ERROR("The bot **needs to be member** of the target guild.")

        bot_perms = bot.permissions_for_guild(guild)
        if not bot_perms.administrator:
            raise ctx.f.ERROR("The bot **needs to have `administrator`** in the target guild.")

    @sync.command(aliases=("channels", "msg"))
    @wkr.guild_only
    @checks.has_permissions_level(destructive=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    async def messages(self, ctx, direction, target: wkr.ChannelConverter):
        """
        Sync messages from one channel to another


        __Arguments__

        **direction**: `from`, `to` or `both`
        **target**: The target channel (mention or id)


        __Examples__

        From the target to this channel: ```{b.prefix}sync messages from #general```
        From this channel to the target: ```{b.prefix}sync messages to #general```
        Both directions: ```{b.prefix}sync messages both #general```
        """
        try:
            direction = getattr(SyncDirection, direction.upper())
        except AttributeError:
            raise ctx.f.ERROR(f"`{direction}` is **not a valid sync direction**.\n"
                              f"Choose from `{', '.join([l.name.lower() for l in SyncDirection])}`.")

        channel = await target(ctx)
        guild = await self.client.get_full_guild(channel.guild_id)
        await self._check_admin_on(guild, ctx)

        async def _create_msg_sync(target_id, source_id):
            webh = await ctx.client.create_webhook(wkr.Snowflake(target_id), name="sync")
            sync_id = utils.unique_id()
            try:
                await ctx.bot.db.premium.syncs.insert_one({
                    "_id": sync_id,
                    "guilds": [guild.id, ctx.guild_id],
                    "type": SyncType.MESSAGES,
                    "target": target_id,
                    "source": source_id,
                    "webhook": webh.to_dict(),
                    "uses": 0
                })
            except pymongo.errors.DuplicateKeyError:
                await ctx.f_send(
                    f"Sync from <#{source_id}> to <#{target_id}> **already exists**.",
                    f=ctx.f.INFO
                )

            else:
                await ctx.f_send(
                    f"Successfully **created sync** from <#{source_id}> to <#{target_id}> with the id `{sync_id}`",
                    f=ctx.f.SUCCESS
                )
                await ctx.bot.create_audit_log(
                    utils.AuditLogType.MESSAGE_SYNC_CREATE, [ctx.guild_id, guild.id], ctx.author.id,
                    {"source": source_id, "target": target_id, "id": sync_id}
                )

        if direction == SyncDirection.FROM or direction == SyncDirection.BOTH:
            await _create_msg_sync(ctx.channel_id, channel.id)

        if direction == SyncDirection.TO or direction == SyncDirection.BOTH:
            await _create_msg_sync(channel.id, ctx.channel_id)

    @wkr.Module.listener()
    async def on_message_create(self, _, data):
        msg = wkr.Message(data)
        if msg.webhook_id:
            return

        attachments = msg.attachments
        files = []

        async def _fetch_attachment(attachment):
            async with self.bot.session.get(attachment["url"]) as resp:
                if resp.status == 200:
                    fp = io.BytesIO(await resp.read())
                    files.append(wkr.File(fp, filename=attachment["filename"]))

        file_tasks = [self.bot.schedule(_fetch_attachment(att)) for att in attachments]
        if file_tasks:
            await asyncio.wait(file_tasks, return_when=asyncio.ALL_COMPLETED)

        syncs = self.bot.db.premium.syncs.find({"source": msg.channel_id, "type": SyncType.MESSAGES})
        async for sync in syncs:
            webh = wkr.Webhook(sync["webhook"])
            try:
                await self.client.execute_webhook(
                    webh,
                    username=msg.author.name,
                    avatar_url=msg.author.avatar_url,
                    files=files,
                    **msg.to_dict(),
                    allowed_mentions={"parse": []}
                )
                await self.bot.db.premium.syncs.update_one(sync, {"$inc": {"uses": 1}})
            except wkr.NotFound:
                await self.bot.db.syncs.delete_one({"_id": sync["_id"]})

            except Exception:
                traceback.print_exc()

    @sync.command()
    @wkr.guild_only
    @checks.has_permissions_level(destructive=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    async def bans(self, ctx, direction, target: wkr.FullGuildConverter):
        """
        Sync bans from one guild to another


        __Arguments__

        **direction**: `from`, `to` or `both`
        **target**: The target guild


        __Examples__

        From the target to this guild: ```{b.prefix}sync bans from 410488579140354049```
        From this guild to the target: ```{b.prefix}sync bans to 410488579140354049```
        Both directions: ```{b.prefix}sync bans both 410488579140354049```
        """
        try:
            direction = getattr(SyncDirection, direction.upper())
        except AttributeError:
            raise ctx.f.ERROR(f"`{direction}` is **not a valid sync direction**.\n"
                              f"Choose from `{', '.join([l.name.lower() for l in SyncDirection])}`.")

        guild = await target(ctx)
        await self._check_admin_on(guild, ctx)

        async def _create_ban_sync(target, source):
            sync_id = utils.unique_id()
            try:
                await ctx.bot.db.premium.syncs.insert_one({
                    "_id": sync_id,
                    "guilds": [guild.id, ctx.guild_id],
                    "type": SyncType.BANS,
                    "target": target.id,
                    "source": source.id,
                    "uses": 0
                })
            except pymongo.errors.DuplicateKeyError:
                await ctx.f_send(
                    f"Sync from {source.name} to {target.name} **already exists**.",
                    f=ctx.f.INFO
                )
                return

            else:
                await ctx.f_send(
                    f"Successfully **created sync** from {source.name} to {target.name} with the id `{sync_id}`.\n"
                    f"The bot will now copy all existing bans.",
                    f=ctx.f.SUCCESS
                )
                await ctx.bot.create_audit_log(
                    utils.AuditLogType.BAN_SYNC_CREATE, [ctx.guild_id, guild.id], ctx.author.id,
                    {"source": source.id, "target": target.id, "id": sync_id}
                )

            async def _copy_bans():
                existing_bans = await ctx.bot.fetch_bans(source)
                for ban in existing_bans:
                    await self.bot.ban_user(target, wkr.Snowflake(ban["user"]["id"]), reason=ban["reason"])

            self.bot.schedule(_copy_bans())

        ctx_guild = await ctx.get_guild()
        if direction == SyncDirection.FROM or direction == SyncDirection.BOTH:
            await _create_ban_sync(ctx_guild, guild)

        if direction == SyncDirection.TO or direction == SyncDirection.BOTH:
            await _create_ban_sync(guild, ctx_guild)

    @wkr.Module.listener()
    async def on_guild_ban_add(self, _, data):
        user = wkr.User(data["user"])
        syncs = self.bot.db.premium.syncs.find({"source": data["guild_id"], "type": SyncType.BANS})
        # guild_ban_add doesn't receive the ban reason
        ban = None
        async for sync in syncs:
            if ban is None:
                ban = await self.bot.fetch_ban(wkr.Snowflake(data["guild_id"]), user)

            try:
                await self.bot.ban_user(wkr.Snowflake(sync["target"]), user, reason=ban["reason"])
                await self.bot.db.premium.syncs.update_one(sync, {"$inc": {"uses": 1}})
            except Exception:
                traceback.print_exc()

    @wkr.Module.listener()
    async def on_guild_ban_remove(self, _, data):
        user = wkr.User(data["user"])
        syncs = self.bot.db.premium.syncs.find({"source": data["guild_id"], "type": SyncType.BANS})
        async for sync in syncs:
            try:
                await self.bot.unban_user(wkr.Snowflake(sync["target"]), user)
                await self.bot.db.premium.syncs.update_one(sync, {"$inc": {"uses": 1}})
            except Exception:
                traceback.print_exc()

    @sync.command(aliases=("members", "assignments"))
    @wkr.guild_only
    @checks.has_permissions_level(destructive=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium(checks.PremiumLevel.THREE)
    async def role(self, ctx, role_a: wkr.RoleConverter, direction, role_b: wkr.RoleConverter):
        """
        Sync role assignments for one role to another role
        The roles can be on different servers


        __Arguments__

        **source_role**: The id of the first role (role A)
        **direction**: `from`, `to` or `both`
        **target**: The id of the second role (role B)


        __Examples__

        Adding role A to member X will also add role B to member X:
        ```{b.prefix}sync role 410288579140354049 from 410488579140354049```
        Adding role B to member X will also add role A to member X:
        ```{b.prefix}sync role 410288579140354049 to 410488579140354049```
        Both combined:
        ```{b.prefix}sync role 410288579140354049 both 410488579140354049```
        """
        try:
            direction = getattr(SyncDirection, direction.upper())
        except AttributeError:
            raise ctx.f.ERROR(f"`{direction}` is **not a valid sync direction**.\n"
                              f"Choose from `{', '.join([l.name.lower() for l in SyncDirection])}`.")

        source = await role_a(ctx)
        source_guild = await ctx.client.get_full_guild(source.guild_id)
        await self._check_admin_on(source_guild, ctx)
        target = await role_b(ctx)
        target_guild = await ctx.client.get_full_guild(target.guild_id)
        await self._check_admin_on(target_guild, ctx)

        async def _create_role_sync(target_role, source_role):
            sync_id = utils.unique_id()
            try:
                await ctx.bot.db.premium.syncs.insert_one({
                    "_id": sync_id,
                    "guilds": [target_guild.id, source_guild.id],
                    "type": SyncType.ROLE,
                    "target": target_role.id,
                    "target_guild": target_role.guild_id,
                    "source": source_role.id,
                    "source_guild": source_role.guild_id,
                    "uses": 0
                })
            except pymongo.errors.DuplicateKeyError:
                await ctx.f_send(
                    f"Sync from {source_role.name} (`{source_role.id}`) to {target_role.name} (`{target_role.id}`) "
                    f"**already exists**.",
                    f=ctx.f.INFO
                )

            else:
                await ctx.f_send(
                    f"Successfully **created sync** from {source_role.name} (`{source_role.id}`) to "
                    f"{target_role.name} (`{target_role.id}`)with the id `{sync_id}`",
                    f=ctx.f.SUCCESS
                )
                await ctx.bot.create_audit_log(
                    utils.AuditLogType.ROLE_SYNC_CREATE, [source_guild.id, target_guild.id], ctx.author.id,
                    {"source": source.id, "target": target.id, "id": sync_id}
                )

        if direction == SyncDirection.FROM or direction == SyncDirection.BOTH:
            await _create_role_sync(source, target)

        if direction == SyncDirection.TO or direction == SyncDirection.BOTH:
            await _create_role_sync(target, source)

    @wkr.Module.listener()
    async def on_guild_member_add(self, _, data):
        await self.client.redis.hset(
            f"role_syncs:{data['guild_id']}",
            data['user']['id'],
            msgpack.packb(data["roles"])
        )

    @wkr.Module.listener()
    async def on_guild_member_update(self, _, data):
        try:
            prev_roles = []
            cached = await self.client.redis.hget(f"role_syncs:{data['guild_id']}", data['user']['id'])
            if cached is not None:
                prev_roles = msgpack.unpackb(cached)

            added_roles = [r for r in data["roles"] if r not in prev_roles]
            removed_roles = [r for r in prev_roles if r not in data["roles"]]

        finally:
            await self.client.redis.hset(
                f"role_syncs:{data['guild_id']}",
                data['user']['id'],
                msgpack.packb(data["roles"])
            )

        for role_id in added_roles:
            syncs = self.bot.db.premium.syncs.find({"source": role_id, "type": SyncType.ROLE})
            async for sync in syncs:
                await self.client.add_role(
                    wkr.Snowflake(sync["target_guild"]),
                    wkr.Snowflake(data["user"]["id"]),
                    wkr.Snowflake(sync["target"]),
                    reason=f"Role sync {sync['_id']}"
                )

        for role_id in removed_roles:
            syncs = self.bot.db.premium.syncs.find({"source": role_id, "type": SyncType.ROLE})
            async for sync in syncs:
                await self.client.remove_role(
                    wkr.Snowflake(sync["target_guild"]),
                    wkr.Snowflake(data["user"]["id"]),
                    wkr.Snowflake(sync["target"]),
                    reason=f"Role sync {sync['_id']}"
                )

    @wkr.Module.listener()
    async def on_guild_member_remove(self, _, data):
        await self.client.redis.hdel(
            f"role_syncs:{data['guild_id']}",
            data['user']['id']
        )

    @wkr.Module.listener()
    async def on_guild_role_delete(self, _, data):
        await self.bot.db.premium.syncs.delete_many({"$or": [{"source": data["role_id"]}, {"target": data["role_id"]}]})

    @wkr.Module.listener()
    async def on_guild_delete(self, _, data):
        await self.bot.db.premium.syncs.delete_many({"guilds": data["id"]})

    @wkr.Module.listener()
    async def on_channel_delete(self, _, data):
        await self.bot.db.premium.syncs.delete_many({"$or": [{"source": data["id"]}, {"target": data["id"]}]})

import traceback
import xenon_worker as wkr
import asyncio
import io


class Options:
    def __init__(self, **default):
        self.all = False
        self.options = default

    def update(self, **options):
        for key, value in options.items():
            if key == "*":
                self.all = value
                self.options.clear()

            else:
                self.options[key] = value

    def __getattr__(self, item):
        return self.get(item)

    def get(self, item):
        if item in self.options.keys():
            return bool(self.options[item])

        return self.all


class BackupSaver:
    def __init__(self, client, guild):
        self.client = client
        self.guild = guild
        self.data = guild.to_dict()
        self.chatlog = None

    async def _save_roles(self):
        self.data["roles"] = [
            r.to_dict()
            for r in self.guild.roles
            if not r.managed
        ]

    async def _save_bans(self):
        self.data["bans"] = [
            {
                "reason": ban["reason"],
                "id": ban["user"]["id"]
            }
            for ban in await self.client.fetch_bans(self.guild)
        ]

    async def _save_members(self):
        self.data["members"] = [
            {
                "id": member.id,
                "nick": member.nick,
                "deaf": member.deaf,
                "mute": member.mute,
                "roles": member.roles
            }
            async for member in self.client.iter_members(self.guild, 10 ** 6)
        ]

    async def _save_messages(self):
        if self.chatlog <= 0:
            return

        messages = self.data["messages"] = {}

        for channel in self.guild.channels:
            if channel.type == wkr.ChannelType.GUILD_VOICE or channel.type == wkr.ChannelType.GUILD_CATEGORY:
                continue

            try:
                messages[channel.id] = [
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
                    async for message in self.client.iter_messages(channel, self.chatlog)
                ]
            except wkr.DiscordException:
                pass

    async def save(self, chatlog=0, **options):
        self.chatlog = chatlog
        savers = {
            "roles": self._save_roles,
            "bans": self._save_bans,
            "members": self._save_members,
            "messages": self._save_messages
        }

        for _, saver in savers.items():
            try:
                await saver()
            except wkr.DiscordException:
                pass


class BackupLoader:
    def __init__(self, client, guild, data, reason="Backup loaded"):
        self.client = client
        self.guild = guild
        self.data = data

        self.chatlog = None
        self.options = Options(
            settings=True,
            roles=True,
            delete_roles=True,
            channels=True,
            delete_channels=True,
            members=True,
            bans=False
        )
        self.id_translator = {data["id"]: guild.id}
        self.reason = reason

        self.status = None

    async def _load_settings(self):
        self.status = "loading settings"

        self.data.pop("guild_id", None)
        await self.client.edit_guild(self.guild, **self.data, reason=self.reason)

    async def _delete_roles(self):
        self.status = "deleting roles"

        existing = [
            r for r in self.guild.roles
            if not r.managed and not r.is_default()
        ]

        for role in sorted(existing, key=lambda r: r.position):
            try:
                await self.client.delete_role(role, reason=self.reason)
            except wkr.Forbidden:
                break

            except wkr.DiscordException:
                pass

    async def _load_roles(self):
        self.status = "loading roles"
        roles = list(sorted(self.data["roles"], key=lambda r: r["position"], reverse=True))
        for role in roles:
            role.pop("guild_id", None)
            role.pop("position", None)
            role.pop("managed", None)

            # Default role (@everyone)
            if role["id"] == self.data["id"]:
                to_edit = self.guild.default_role
                if to_edit is not None:
                    try:
                        await self.client.edit_role(to_edit, **role, reason=self.reason)
                        self.id_translator[role["id"]] = to_edit.id

                    except wkr.DiscordException:
                        traceback.print_exc()

                continue

            try:
                new = await asyncio.wait_for(
                    self.client.create_role(self.guild, **role, reason=self.reason),
                    timeout=15
                )
            except asyncio.TimeoutError:
                raise self.client.f.ERROR("Seems like you **hit** the `250 per 24h` **role creation limit** of "
                                          "discord.\nYou have to **wait for 24 hours** until you can load another "
                                          "backup or template.\n\n"
                                          "*This is a discord limitation and there is no way around it.*")

            except wkr.DiscordException:
                traceback.print_exc()
                continue

            self.id_translator[role["id"]] = new.id

    async def _delete_channels(self):
        self.status = "deleting channels"

        for channel in self.guild.channels:
            try:
                await self.client.delete_channel(channel, reason=self.reason)
            except wkr.DiscordException:
                traceback.print_exc()

    async def _load_channels(self):
        self.status = "loading channels"

        def _tune_channel(channel):
            channel.pop("guild_id", None)

            # Bitrates over 96000 require special features or boosts
            # (boost advantages change a lot, so we just ignore them)
            if "bitrate" in channel.keys() and "VIP_REGIONS" not in self.guild.features:
                channel["bitrate"] = min(channel["bitrate"], 96000)

            # News and store channels require special features
            if (channel["type"] == wkr.ChannelType.GUILD_NEWS and "NEWS" not in self.guild.features) or \
                    (channel["type"] == wkr.ChannelType.GUILD_STORE and "COMMERCE" not in self.guild.features):
                channel["type"] = 0

            channel["type"] = 0 if channel["type"] > 4 else channel["type"]

            if "parent_id" in channel.keys():
                channel["parent_id"] = self.id_translator.get(channel["parent_id"], channel["parent_id"])

            overwrites = channel.get("permission_overwrites", [])
            for overwrite in overwrites:
                if overwrite["id"] in self.id_translator:
                    overwrite["id"] = self.id_translator[overwrite["id"]]

            return channel

        no_parent = sorted(
            filter(lambda c: c.get("parent_id") is None, self.data["channels"]),
            key=lambda c: c.get("position")
        )
        for channel in no_parent:
            try:
                new = await self.client.create_channel(self.guild, **_tune_channel(channel), reason=self.reason)
                self.id_translator[channel["id"]] = new.id
            except wkr.DiscordException:
                traceback.print_exc()

        has_parent = sorted(
            filter(lambda c: c.get("parent_id") is not None, self.data["channels"]),
            key=lambda c: c["position"]
        )
        for channel in has_parent:
            try:
                new = await self.client.create_channel(self.guild, **_tune_channel(channel), reason=self.reason)
                self.id_translator[channel["id"]] = new.id
            except wkr.DiscordException:
                traceback.print_exc()

    async def _load_bans(self):
        self.status = "loading bans"

        for ban in self.data.get("bans", []):
            try:
                await self.client.ban_user(self.guild, wkr.Snowflake(ban["id"]), reason=ban["reason"])
            except wkr.DiscordException:
                pass

    async def _load_members(self):
        self.status = "loading members"

        to_load = {m["id"]: m for m in self.data.get("members", [])}
        async for member in self.client.iter_members(self.guild, 10 ** 6):
            data = to_load.get(member.id)
            if data is None:
                continue

            roles = list(member.roles)
            for role in data["roles"]:
                new_id = self.id_translator.get(role)
                if new_id is not None:
                    roles.append(new_id)

            if len(roles) != len(member.roles):
                try:
                    await self.client.edit_member(
                        self.guild,
                        member,
                        nick=data.get("nick"),
                        roles=roles
                    )
                except wkr.DiscordException:
                    pass

    async def _load_messages(self):
        self.status = "loading messages"

        # Sending messages to too many webhooks at the same time gets you ip banned pretty fast
        semaphore = asyncio.Semaphore(value=5)

        async def _load_in_channel(channel):
            messages = self.data.get("messages", {}).get(channel["id"], [])
            if len(messages) <= 0:
                return

            new_id = self.id_translator.get(channel["id"])
            if new_id is None:
                return

            try:
                webhook = await self.client.create_webhook(wkr.Snowflake(new_id), name="backup")
                for msg in reversed(messages[:self.chatlog]):
                    author = wkr.User(msg["author"])

                    attachments = msg.get("attachments", [])
                    files = []

                    async def _fetch_attachment(attachment):
                        async with self.client.session.get(attachment["url"]) as resp:
                            if resp.status == 200:
                                fp = io.BytesIO(await resp.read())
                                files.append(wkr.File(fp, filename=attachment["filename"]))

                    file_tasks = [self.client.schedule(_fetch_attachment(att)) for att in attachments]
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

                    except asyncio.CancelledError:
                        raise

                    except:
                        traceback.print_exc()

                await self.client.delete_webhook(webhook)
            finally:
                semaphore.release()

        for _channel in self.data["channels"]:
            await semaphore.acquire()
            self.client.schedule(_load_in_channel(_channel))

    async def _load(self, chatlog, **options):
        self.chatlog = chatlog
        self.options.update(**options)
        await self.client.edit_guild(self.guild, name="Loading ...")
        loaders = (
            ("delete_roles", self._delete_roles),
            ("roles", self._load_roles),
            ("delete_channels", self._delete_channels),
            ("channels", self._load_channels),
            ("bans", self._load_bans),
            ("members", self._load_members),
            ("channels", self._load_messages),
            ("settings", self._load_settings)
        )

        for key, loader in loaders:
            if self.options.get(key):
                try:
                    await loader()
                except wkr.CommandError:
                    raise
                except wkr.DiscordException:
                    traceback.print_exc()

        await self.client.edit_guild(self.guild, name=self.data["name"])

    async def load(self, chatlog, **options):
        self.status = "starting"

        redis_key = f"loaders:{self.guild.id}"
        if await self.client.redis.exists(redis_key):
            # Another loader is already running
            raise self.client.f.ERROR("There is **already** a backup or template loader **running**. "
                                      "You can't start more than one at the same time.\n"
                                      "You have to **wait until it's done**.")

        task = self.client.schedule(self._load(chatlog, **options))
        while not task.done():
            await self.client.redis.setex(redis_key, 10, self.status)
            await asyncio.sleep(5)
            if not await self.client.redis.exists(redis_key):
                # The loading key got deleted, probably manual cancellation
                task.cancel()
                raise self.client.f.ERROR("The **loading process was cancelled**. Did you cancel it manually?")

        return task.result()

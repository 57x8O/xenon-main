import xenon_worker as wkr
import asyncio

import checks
import utils
from backups import BackupSaver, BackupLoader


class Copy(wkr.Module):
    @wkr.Module.command()
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    @wkr.cooldown(1, 60, bucket=wkr.CooldownType.GUILD)
    async def copy(self, ctx, guild_id: wkr.FullGuildConverter, chatlog: int = 0, *options):
        """
        Copy a server without creating a backup


        **guild_id**: The id of the guild to copy from
        **chatlog**: The count of messages to copy per channel
        """
        source_guild = await guild_id(ctx)
        target_guild = await ctx.get_full_guild()

        bot_member = await ctx.client.get_bot_member(source_guild.id)
        if bot_member is None:
            raise wkr.BotMissingPermissions(("administrator",))

        bot_permissions = bot_member.permissions_for_guild(source_guild)
        if not bot_permissions.administrator:
            raise wkr.BotMissingPermissions(("administrator",))

        try:
            member = await ctx.client.fetch_member(source_guild, ctx.author.id)
        except wkr.NotFound:
            raise wkr.Permissions(("administrator",))

        permissions = member.permissions_for_guild(source_guild)
        if not permissions.administrator:
            raise wkr.Permissions(("administrator",))

        if ctx.premium == checks.PremiumLevel.ONE:
            chatlog = min(chatlog, 50)

        elif ctx.premium == checks.PremiumLevel.TWO:
            chatlog = min(chatlog, 100)

        elif ctx.premium == checks.PremiumLevel.THREE:
            chatlog = min(chatlog, 250)

        warning_msg = await ctx.f_send("Are you sure that you want to copy this guild?\n"
                                       f"Please put the managed role called `{ctx.bot.user.name}` above all other "
                                       f"roles before clicking the ✅ reaction.\n\n"
                                       "__**All channels and roles will get replaced!**__\n\n"
                                       "*Also keep in mind that you can only copy up to 250 roles per day.*", f=ctx.f.WARNING)
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

        status_msg = await ctx.f_send("**Preparing to copy** ...", f=ctx.f.WORKING)
        backup = BackupSaver(ctx.client, source_guild)
        await backup.save(chatlog)

        await ctx.client.edit_message(status_msg, **ctx.f.format(f"**Starting to copy** ...", f=ctx.f.WORKING))

        loader = BackupLoader(ctx.client, target_guild, backup.data, reason="Copied by " + str(ctx.author))

        translator = await ctx.bot.db.id_translators.find_one({"target_id": ctx.guild_id, "source_id": loader.data["id"]})
        if translator is not None:
            loader.id_translator.update(translator["ids"])

        await loader.load(chatlog, **utils.backup_options(options))

        unpacked_ids = {
            f"ids.{s}": t
            for s, t in loader.id_translator.items()
        }
        await ctx.bot.db.id_translators.update_one(
            {
                "target_id": ctx.guild_id,
                "source_id": loader.data["id"],
            },
            {"$set": {
                "target_id": ctx.guild_id,
                "source_id": loader.data["id"],
                **unpacked_ids
            }},
            upsert=True
        )

    @copy.command(aliases=('msg',))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @checks.is_premium()
    @wkr.cooldown(1, 60, bucket=wkr.CooldownType.GUILD)
    async def messages(self, ctx, channel_id: wkr.ChannelConverter, count: int = 100):
        """
        Copy messages from one channel to another without creating a chatlog


        **channel_id**: The id of the channel to copy from
        **count**: The count of messages to copy
        """
        channel = await channel_id(ctx)

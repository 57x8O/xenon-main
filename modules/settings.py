import xenon_worker as wkr
from datetime import datetime, timedelta

import checks

PERMISSION_DESCRIPTIONS = {
    checks.PermissionLevels.ADMIN_ONY: "Server admins can create backups, enable the backup interval and "
                                       "load a template or backup",
    checks.PermissionLevels.DESTRUCTIVE_OWNER: "Server admins can create backups and enable the backup interval "
                                               "but only the server owner can load backups or templates",
    checks.PermissionLevels.OWNER_ONLY: "Only the server owner can use any of the relevant commands"
}


class Settings(wkr.Module):
    @wkr.Module.task(hours=1)
    async def audit_log_retention(self):
        await self.bot.db.audit_logs.delete_many({
            "timestamp": {
                "$lte": datetime.utcnow() - timedelta(days=365)
            }
        })

    @wkr.Module.command(aliases=("set",))
    @wkr.guild_only
    @wkr.is_owner
    @wkr.cooldown(1, 5, bucket=wkr.CooldownType.GUILD)
    async def settings(self, ctx):
        """
        Change Xenons settings for your server

        Get more help on the [wiki](https://wiki.xenon.bot/en/settings).
        """
        await ctx.invoke("help settings")

    @settings.command()
    @wkr.cooldown(1, 30, bucket=wkr.CooldownType.GUILD)
    async def reset(self, ctx):
        """
        Reset the server settings to the default values


        __Examples__

        ```{b.prefix}settings reset```
        """
        await ctx.bot.db.guilds.delete_one({"_id": ctx.guild_id})
        raise ctx.f.SUCCESS(f"Successfully **reset settings** to the default values.")

    @settings.command(aliases=("perms", "permission"))
    @wkr.cooldown(1, 5, bucket=wkr.CooldownType.GUILD)
    async def permissions(self, ctx, *, level=None):
        """
        Set the permissions level for your server

        Get more help on the [wiki](https://wiki.xenon.bot/en/settings#permissions-settings).

        This affects the following commands:
        `backup load`, `backup create`, `template load`, `backup interval`, `chatlog create`, `chatlog load`, `copy`

        __Levels__

        Server admins can create backups, enable the backup interval and load a template or backup (**Be careful with this!**):
        ```{b.prefix}settings permissions admins```

        Admins can create backups and enable the backup interval but only owners can load backups or templates
        ```{b.prefix}settings permissions destructive owner```

        Only owners can create backups, enable the backup interval and load a template or backup
        ```{b.prefix}settings permissions owner```
        """
        if level is None:
            settings = await ctx.bot.db.guilds.find_one({"_id": ctx.guild_id})
            if settings is None or "permissions_level" not in settings:
                level = checks.PermissionLevels.DESTRUCTIVE_OWNER

            else:
                level = checks.PermissionLevels(settings["permissions_level"])

            raise ctx.f.INFO(f"__Your current permission settings are:__\n"
                             f"{PERMISSION_DESCRIPTIONS[level]}\n\n"
                             f"*Use `{ctx.bot.prefix}help settings permissions` to get more info.*")

        else:
            level = level.replace("-", " ").replace("_", " ")
            if level == "admins":
                conf_level = checks.PermissionLevels.ADMIN_ONY
                await ctx.f_send("__Changed the permissions level for this server to:__\n"
                                 f"**{PERMISSION_DESCRIPTIONS[conf_level]}**\n\n"
                                 f"*Use `{ctx.bot.prefix}help settings permissions` to get more info.*",
                                 f=ctx.f.SUCCESS)

            elif level == "destructive owner":
                conf_level = checks.PermissionLevels.DESTRUCTIVE_OWNER
                await ctx.f_send("__Changed the permissions level for this server to:__\n"
                                 f"**{PERMISSION_DESCRIPTIONS[conf_level]}**.\n\n"
                                 f"*Use `{ctx.bot.prefix}help settings permissions` to get more info.*",
                                 f=ctx.f.SUCCESS)

            elif level == "owner":
                conf_level = checks.PermissionLevels.OWNER_ONLY
                await ctx.bot.db.intervals.delete_many({"guild": ctx.guild_id, "user": {"$ne": ctx.author.id}})
                await ctx.f_send("__Changed the permissions level for this server to:__\n"
                                 f"**{PERMISSION_DESCRIPTIONS[conf_level]}**.\n\n"
                                 f"*Use `{ctx.bot.prefix}help settings permissions` to get more info.*",
                                 f=ctx.f.SUCCESS)

            else:
                await ctx.f_send(f"`{level}` is **not** a **valid** permissions level.", f=ctx.f.ERROR)
                await ctx.invoke("help settings permissions")
                return

            await ctx.bot.db.guilds.update_one(
                {"_id": ctx.guild_id},
                {"$set": {"_id": ctx.guild_id, "permissions_level": conf_level}},
                upsert=True
            )

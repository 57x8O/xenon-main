import xenon_worker as wkr
from enum import IntEnum
from os import environ as env


class StaffLevel(IntEnum):
    NONE = -1
    MOD = 0
    ADMIN = 1


class NotStaff(wkr.CheckFailed):
    def __init__(self, current=StaffLevel.NONE, required=StaffLevel.MOD):
        self.current = current
        self.required = required


def is_staff(level=StaffLevel.MOD):
    def predicate(callback):
        async def check(ctx, *args, **kwargs):
            staff = await ctx.bot.db.staff.find_one({"_id": ctx.author.id})
            if staff is None:
                raise NotStaff(required=level)

            if staff["level"] < level.value:
                raise NotStaff(current=StaffLevel(staff["level"]), required=level)

            return True

        return wkr.Check(check, callback)

    return predicate


class PermissionLevels(IntEnum):
    ADMIN_ONY = 0
    DESTRUCTIVE_OWNER = 1
    OWNER_ONLY = 2


def has_permissions_level(destructive=False):
    def predicate(callback):
        async def check(ctx, *args, **kwargs):
            settings = await ctx.bot.db.guilds.find_one({"_id": ctx.guild_id})
            if settings is None or "permissions_level" not in settings:
                required = PermissionLevels.DESTRUCTIVE_OWNER

            else:
                required = PermissionLevels(settings["permissions_level"])

            if required == PermissionLevels.OWNER_ONLY:
                try:
                    return await wkr.is_owner(callback).run(ctx, *args, **kwargs)
                except wkr.NotOwner:
                    raise ctx.f.ERROR("Only the **server owner** can use this command.\n"
                                      f"The server owner can change this using "
                                      f"`{ctx.bot.prefix}help settings permissions`.")

            elif required == PermissionLevels.DESTRUCTIVE_OWNER and destructive:
                try:
                    return await wkr.is_owner(callback).run(ctx, *args, **kwargs)
                except wkr.NotOwner:
                    raise ctx.f.ERROR("Only the **server owner** can use this command.\n"
                                      f"The server owner can change this using "
                                      f"`{ctx.bot.prefix}help settings permissions`.")

            else:
                try:
                    return await wkr.has_permissions(administrator=True)(callback).run(ctx, *args, **kwargs)
                except wkr.MissingPermissions:
                    raise

        return wkr.Check(check, callback)

    return predicate


SUPPORT_GUILD = env.get("SUPPORT_GUILD")


class PremiumLevel(IntEnum):
    NONE = 0
    ONE = 1
    TWO = 2
    THREE = 3


class NotPremium(wkr.CheckFailed):
    def __init__(self, current=PremiumLevel.NONE, required=PremiumLevel.ONE):
        self.current = current
        self.required = required


def is_premium(level=PremiumLevel.ONE):
    def predicate(callback):
        async def check(ctx, *args, **kwargs):
            try:
                member = await ctx.bot.fetch_member(wkr.Snowflake(SUPPORT_GUILD), ctx.author.id)
            except wkr.NotFound:
                raise NotPremium(required=level)

            guild = await ctx.bot.fetch_guild(SUPPORT_GUILD)
            roles = member.roles_from_guild(guild)

            current = 0
            prefix = "Premium "
            for role in roles:
                if role.name.startswith(prefix):
                    try:
                        value = int(role.name.strip(prefix))
                    except ValueError:
                        continue

                    if value > current:
                        current = value

            current_level = PremiumLevel(current)
            ctx.premium = current_level

            if current < level.value:
                raise NotPremium(current=current_level, required=level)

        return wkr.Check(check, callback)

    return predicate

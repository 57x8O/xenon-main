import xenon_worker as wkr
from enum import IntEnum


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
                    raise

            elif required == PermissionLevels.DESTRUCTIVE_OWNER and destructive:
                try:
                    return await wkr.is_owner(callback).run(ctx, *args, **kwargs)
                except wkr.NotOwner:
                    raise

            else:
                try:
                    return await wkr.has_permissions(administrator=True)(callback).run(ctx, *args, **kwargs)
                except wkr.MissingPermissions:
                    raise

        return wkr.Check(check, callback)

    return predicate

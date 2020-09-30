import xenon_worker as wkr
import modules
from datetime import datetime

import checks


class Xenon(wkr.RabbitBot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = self.mongo.xenon
        for module in modules.to_load:
            self.add_module(module(self))

    async def create_audit_log(self, type, guild_ids, user_id, extra=None):
        return await self.db.audit_logs.insert_one({
            "type": type.value,
            "timestamp": datetime.utcnow(),
            "guilds": guild_ids,
            "user": user_id,
            "extra": extra or {}
        })

    async def on_command_error(self, shard_id, cmd, ctx, e):
        if isinstance(e, checks.NotStaff):
            await ctx.f_send(
                f"This command **can only be used by users with the staff level `{e.required.name}` or higher**.\n"
                f"Your current staff level is `{e.current.name}`.",
                f=ctx.f.ERROR
            )

        elif isinstance(e, checks.NotPremium):
            await ctx.f_send(
                f"This command **can only be used by users with the premium level `{e.required.name}` or higher**.\n"
                f"Your current premium level is `{e.current.name}`. "
                f"[Upgrade Here](https://www.patreon.com/merlinfuchs)",
                f=ctx.f.ERROR
            )

        else:
            await super().on_command_error(shard_id, cmd, ctx, e)

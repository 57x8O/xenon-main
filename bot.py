import xenon_worker as wkr
import modules

import checks


class Xenon(wkr.RabbitBot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = self.mongo.xenon
        for module in modules.to_load:
            self.add_module(module(self))

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

    async def start(self, *args, **kwargs):
        await super().start(*args, **kwargs)
        await self.redis.select(1)

import xenon_worker as wkr


class Premium(wkr.Module):
    @wkr.Module.command(aliases=("chatlog", "chatlogs"))
    @wkr.cooldown(1, 3, bucket=wkr.CooldownType.AUTHOR)
    async def chatlog(self, ctx):
        """
        Save & load messages from individual channels
        
        You can find more help on the [wiki](https://wiki.xenon.bot/chatlog)
        """
        await ctx.f_send("This command can **only** be used with **Xenon Premium**.", f=ctx.f.INFO)
        await ctx.invoke("premium")

    @wkr.Module.command()
    @wkr.cooldown(1, 3, bucket=wkr.CooldownType.AUTHOR)
    async def sync(self, ctx):
        """
        Sync messages and bans between different servers and channels
        
        You can find more help on the [wiki](https://wiki.xenon.bot/sync)
        """
        await ctx.f_send("This command can **only** be used with **Xenon Premium**.", f=ctx.f.INFO)
        await ctx.invoke("premium")

    @wkr.Module.command()
    @wkr.cooldown(1, 3, bucket=wkr.CooldownType.AUTHOR)
    async def copy(self, ctx):
        """
        Copy servers without creating a backup
        """
        await ctx.f_send("This command can **only** be used with **Xenon Premium**.", f=ctx.f.INFO)
        await ctx.invoke("premium")

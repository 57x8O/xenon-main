import xenon_worker as wkr
import inspect
import pymongo
from datetime import timedelta, datetime
from contextlib import redirect_stdout
import textwrap
import io

import utils
import checks


class StaffListMenu(wkr.ListMenu):
    embed_kwargs = {"title": "Staff List"}

    async def get_items(self):
        args = {
            "limit": 10,
            "skip": self.page * 10,
            "sort": [("level", pymongo.DESCENDING)],
            "filter": {}
        }
        staff_list = self.ctx.bot.db.staff.find(**args)
        items = []
        async for staff in staff_list:
            user = await self.ctx.client.fetch_user(staff["_id"])
            items.append((
                checks.StaffLevel(staff["level"]).name.lower(),
                str(user)
            ))

        return items


class Admin(wkr.Module):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_exec = None

    @wkr.Module.command(hidden=True)
    @wkr.is_bot_owner
    async def eval(self, ctx, *, expression):
        """
        Evaluate a singe expression and get the result
        """
        if expression.startswith("await "):
            expression = expression[6:]

        try:
            res = eval(expression)
            if inspect.isawaitable(res):
                res = await res

        except Exception as e:
            res = f"{type(e).__name__}: {str(e)}"

        raise ctx.f.SUCCESS(f"```{res}```")

    @wkr.Module.command(hidden=True)
    @wkr.is_bot_owner
    async def exec(self, ctx, *, code):
        """
        Executes a code snipped
        It's internally wrapped in an async function to provide async functionalities
        """
        # Remove code blocks
        if code.startswith('```') and code.endswith('```'):
            code = '\n'.join(code.split('\n')[1:-1])

        code = code.strip("` \n")

        # Make some values available
        env = {
            'bot': self.bot,
            'client': self.client,
            'ctx': ctx,
            'wkr': wkr,
            'imp': __import__,  # Simple shortcut for importing modules
            '_': self._last_exec  # Last exec result
        }
        env.update(globals())

        # Wrap code in async function to provide async functionalities
        to_compile = f'async def func():\n{textwrap.indent(code, "  ")}'

        try:
            exec(to_compile, env)

        except Exception as e:
            raise ctx.f.INFO(f"```py\n{e.__class__.__name__}:\n{e}\n```")

        # The async function is now available in the environment
        func = env["func"]

        # Store stdout of the async def
        stdout = io.StringIO()
        try:
            with redirect_stdout(stdout):
                ret = await func()

        except Exception as e:
            raise ctx.f.INFO(f"```\n{stdout.getvalue() or 'No Stdout'}\n```\n```py\n{e.__class__.__name__}:\n{e}\n```")

        value = stdout.getvalue()
        if ret is None:
            if value:
                raise ctx.f.INFO(f"```{value}```")

        else:
            self._last_exec = ret
            raise ctx.f.INFO(f"```\n{value or 'No Stdout'}\n```\n```py\n{ret}\n```")

    @wkr.Module.command(hidden=True)
    @checks.is_staff(level=checks.StaffLevel.ADMIN)
    async def su(self, ctx, member: wkr.MemberConverter, *, command):
        member = await member(ctx)
        ctx.msg.author = member
        await ctx.invoke(command)

    @wkr.Module.command(hidden=True)
    @checks.is_staff(level=checks.StaffLevel.MOD)
    async def gateway(self, ctx):
        bot_gw = await ctx.bot.bot_gateway()
        identifies = bot_gw["session_start_limit"]

        reset_after = timedelta(milliseconds=identifies["reset_after"])
        reset = datetime.utcnow() + reset_after

        raise ctx.f.INFO(embed={
            "fields": [
                {
                    "name": "Url",
                    "value": bot_gw["url"],
                    "inline": True
                },
                {
                    "name": "Recommended Shards",
                    "value": bot_gw["shards"],
                    "inline": True
                },
                {
                    "name": "Total Identifies",
                    "value": identifies["total"],
                    "inline": True
                },
                {
                    "name": "Remaining Identifies",
                    "value": identifies["remaining"],
                    "inline": True
                },
                {
                    "name": "Reset After",
                    "value": f"{utils.timedelta_to_string(reset_after)} ({utils.datetime_to_string(reset)} UTC)",
                    "inline": True
                }
            ]
        })

    @wkr.Module.command(hidden=True)
    @checks.is_staff()
    async def staff(self, ctx):
        """
        List all staff users
        (does not include owners)
        """
        menu = StaffListMenu(ctx)
        await menu.start()

    @staff.command(keep_checks=False)
    @wkr.is_bot_owner
    async def add(self, ctx, user: wkr.UserConverter, level="mod"):
        """
        Add a user to the staff list


        __Arguments__

        **user**: The id of the user to add to the staff list
        **level**: The staff level (e.g. admin)


        __Examples__

        Admin: ```{b.prefix}staff add 386861188891279362 admin```
        Mod: ```{b.prefix}staff add 386861188891279362 mod```
        """
        user = await user(ctx)
        try:
            level = getattr(checks.StaffLevel, level.upper())
        except AttributeError:
            raise ctx.f.ERROR(f"`{level}` is **not a valid staff level**.\n"
                              f"Choose from `{', '.join([l.name.lower() for l in checks.StaffLevel])}`.")

        await ctx.bot.db.staff.update_one({"_id": user.id}, {"$set": {"level": level.value}}, upsert=True)
        raise ctx.f.SUCCESS(f"Successfully **added `{user}` to the staff list**.")

    @staff.command(keep_checks=False, aliases=("rm",))
    @wkr.is_bot_owner
    async def remove(self, ctx, user: wkr.UserConverter):
        """
        Remove a user from the staff list


        __Arguments__

        **user**: The id of the user to remove from the staff list


        __Examples__

        ```{b.prefix}staff add 386861188891279362```
        """
        user = await user(ctx)
        result = await ctx.bot.db.staff.delete_one({"_id": user.id})
        if result.deleted_count > 0:
            raise ctx.f.SUCCESS(f"Successfully **removed `{user}` from the staff list**.")

        else:
            raise ctx.f.ERROR(f"`{user}` is **not in the staff list**.")

    @wkr.Module.command(hidden=True)
    async def loader(self, ctx, guild_id=None):
        guild_id = guild_id or ctx.guild_id
        status = await ctx.bot.redis.get(f"loaders:{guild_id}")
        if status is None:
            raise ctx.f.INFO(f"There is **no loader running** on the guild `{guild_id}`.")

        raise ctx.f.INFO(f"Current **loader status** on the guild `{guild_id}`:```{status.decode('utf-8')}```")

    @loader.command(aliases=("cancel", "abort"))
    @checks.is_staff()
    async def stop(self, ctx, guild_id=None):
        guild_id = guild_id or ctx.guild_id
        await ctx.bot.redis.delete(f"loaders:{guild_id}")
        raise ctx.f.SUCCESS(f"**Cancelled loader** on guild `{guild_id}`.")

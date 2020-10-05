import xenon_worker as wkr
import pymongo
from datetime import datetime

import checks
import utils


class BlackListMenu(wkr.ListMenu):
    embed_kwargs = {"title": "Blacklisted Users"}

    async def get_items(self):
        args = {
            "limit": 10,
            "skip": self.page * 10,
            "sort": [("timestamp", pymongo.DESCENDING)],
            "filter": {}
        }
        blacklist = self.ctx.bot.db.blacklist.find(**args)
        items = []
        async for entry in blacklist:
            try:
                user = await self.ctx.client.fetch_user(entry["_id"])
            except wkr.NotFound:
                name = entry["_id"]
            else:
                name = f"{user} ({user.id})"

            items.append((
                name,
                f"```{entry['reason']}``` by <@{entry['staff']}> (`{utils.datetime_to_string(entry['timestamp'])} UTC`)"
            ))

        return items


async def is_blacklisted(ctx, *args, **kwargs):
    entry = await ctx.bot.db.blacklist.find_one({"_id": ctx.author.id})
    if entry is None:
        return True

    raise ctx.f.ERROR(f"You are **no longer allowed to use this bot** for the following reason:\n"
                      f"```{entry['reason']}```")


class Blacklist(wkr.Module):
    @wkr.Module.listener()
    async def on_load(self, *_, **__):
        # Add the top level blacklist check
        await self.bot.db.backups.create_index([("timestamp", pymongo.ASCENDING)])
        self.client.add_check(wkr.Check(is_blacklisted))

    @wkr.Module.command(hidden=True, aliases=("bl",))
    @checks.is_staff(level=checks.StaffLevel.MOD)
    async def blacklist(self, ctx, user: wkr.UserConverter = None):
        if user is None:
            menu = BlackListMenu(ctx)
            return await menu.start()

        user = await user(ctx)
        entry = await ctx.client.db.blacklist.find_one({"_id": user.id})
        if entry is None:
            raise ctx.f.ERROR(f"{user.mention} **is not on the blacklist**.")

        raise ctx.f.INFO(embed={
            "author": {
                "name": str(user)
            },
            "fields": [
                {
                    "name": "Reason",
                    "value": entry["reason"]
                },
                {
                    "name": "Staff",
                    "value": f"<@{entry['staff']}>"
                },
                {
                    "name": "Timestamp",
                    "value": utils.datetime_to_string(entry["timestamp"]) + " UTC"
                }
            ]
        })

    @blacklist.command()
    async def add(self, ctx, user: wkr.UserConverter, *, reason):
        user = await user(ctx)
        await ctx.client.db.blacklist.replace_one({"_id": user.id}, {
            "_id": user.id,
            "timestamp": datetime.utcnow(),
            "staff": ctx.author.id,
            "reason": reason
        }, upsert=True)
        raise ctx.f.SUCCESS(f"Successfully **added {user.mention} to the blacklist**.")

    @blacklist.command(aliases=("rm",))
    async def remove(self, ctx, user: wkr.UserConverter):
        user = await user(ctx)
        result = await ctx.client.db.blacklist.delete_one({"_id": user.id})
        if result.deleted_count == 0:
            raise ctx.f.ERROR(f"{user.mention} **is not on the blacklist**.")

        raise ctx.f.SUCCESS(f"Successfully **removed {user.mention} from the blacklist**.")

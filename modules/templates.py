import xenon_worker as wkr
import utils
import asyncio
import pymongo
import pymongo.errors
from os import environ as env
import msgpack

from backups import BackupLoader


class TemplateListMenu(wkr.ListMenu):
    embed_kwargs = {
        "title": "Template List",
        "description": "You can find more and more recent template on https://templates.xenon.bot/"
    }

    def __init__(self, ctx, search):
        super().__init__(ctx)
        self.search = search.strip()

    async def get_items(self):
        args = {
            "limit": 10,
            "skip": self.page * 10,
            "sort": [("upvote_count", pymongo.DESCENDING), ("usage_count", pymongo.DESCENDING)],
            "filter": {
                "approved": True,
                "internal": True
            }
        }
        if self.search != "":
            args["filter"]["$text"] = {
                "$search": self.search
            }

        templates = self.ctx.bot.mongo.dtpl.templates.find(**args)
        items = []
        async for template in templates:
            items.append((
                template["name"],
                template.get("description") or "No Description"
            ))

        return items


class Templates(wkr.Module):
    APPROVAL_CHANNEL = env.get("TPL_APPROVAL_CHANNEL")
    LIST_CHANNEL = env.get("TPL_LIST_CHANNEL")
    FEATURED_CHANNEL = env.get("TPL_FEATURED_CHANNEL")
    APPROVAL_GUILD = env.get("TPL_APPROVAL_GUILD")
    APPROVAL_OPTIONS = {}

    @wkr.Module.listener()
    async def on_load(self, *_, **__):
        pass
        # Handled by the templates site
        # await self.bot.db.templates.create_index([("name", pymongo.TEXT), ("description", pymongo.TEXT)])
        # await self.bot.db.templates.create_index([("approved", pymongo.ASCENDING)])
        # await self.bot.db.templates.create_index([("featured", pymongo.ASCENDING)])
        # await self.bot.db.templates.create_index([("uses", pymongo.ASCENDING)])
        # await self.bot.db.templates.create_index([("name", pymongo.ASCENDING)], unique=True)

    async def _crossload_template(self, template_id):
        template_id = template_id.strip("/").split("/")[-1]
        try:
            data = await self.client.http.request(wkr.Route("GET", "/guilds/templates/" + template_id))
            guild = data["serialized_source_guild"]
            return {
                "name": data["name"],
                "description": data["description"],
                "creator_id": data["creator_id"],
                "usage_count": data["usage_count"],
                "approved": True,
                "data": {
                    "id": data["source_guild_id"],
                    "roles": [
                        {
                            "position": pos,
                            **r
                        }
                        for pos, r in enumerate(guild.pop("roles", []))
                    ],
                    "mfa_level": 0,
                    **guild
                }
            }
        except wkr.NotFound:
            return None

    @wkr.Module.command(aliases=("temp", "tpl"))
    async def template(self, ctx):
        """
        Create & load **PUBLIC** templates
        """
        await ctx.invoke("help template")

    @template.command(aliases=("c",))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @wkr.cooldown(1, 30)
    async def create(self, ctx, name, *, description):
        """
        Create a **PUBLIC** template from this server
        Use `{b.prefix}backup create` if you simply want to save or clone your server.


        __Examples__

        ```{b.prefix}template create starter A basic template for new servers```
        """
        raise ctx.f.ERROR("This command is disabled. Please use https://templates.xenon.bot to add new templates, "
                          "you can find help on the [wiki](https://wiki.xenon.bot/en/templates#creating-a-template) "
                          "for how to create new templates.")

    @template.command(aliases=("l",))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.bot_has_permissions(administrator=True)
    @wkr.cooldown(1, 60, bucket=wkr.CooldownType.GUILD)
    async def load(self, ctx, name, *options):
        """
        Load a template

        You can find more help on the [wiki](https://wiki.xenon.bot/templates#loading-a-template).


        __Arguments__

        **name**: The name of the template
        **options**: A list of options (See examples)


        __Examples__

        Default options: ```{b.prefix}template load starter```
        Only roles: ```{b.prefix}template load starter !* roles```
        Everything but bans: ```{b.prefix}template load starter !bans```
        """
        template = await ctx.client.mongo.dtpl.templates.find_one_and_update({
            "internal": True,
            "$or": [{"name": name}, {"_id": name}]
        }, {"$inc": {"usage_count": 1}})
        if template is None:
            template = await self._crossload_template(name)

        if template is None:
            raise ctx.f.ERROR(f"There is **no template** with the name `{name}`.")

        warning_msg = await ctx.f_send("Are you sure that you want to load this template?\n"
                                       f"Please put the managed role called `{ctx.bot.user.name}` above all other "
                                       f"roles before clicking the ✅ reaction.\n\n"
                                       "__**All channels and roles will get replaced!**__\n\n"
                                       "*Also keep in mind that you can only load up to 250 roles per day.*",
                                       f=ctx.f.WARNING)

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

        guild = await ctx.get_full_guild()
        backup = BackupLoader(ctx.client, guild, template["data"], reason="Template loaded by " + str(ctx.author))

        options = list(options)
        options.extend(["!settings", "!members"])
        await self.client.redis.publish("loaders:start", msgpack.packb({
            "id": ctx.guild_id,
            "type": "template",
            "source_id": str(backup.data["id"]),
            "template_id": name.strip("/").split("/")[-1]
        }))
        try:
            await backup.load(**utils.backup_options(options))

            unpacked_ids = {
                f"ids.{s}": t
                for s, t in backup.id_translator.items()
            }
            await ctx.bot.db.id_translators.update_one(
                {
                    "target_id": ctx.guild_id,
                    "source_id": backup.data["id"],
                },
                {
                    "$set": {
                        "target_id": ctx.guild_id,
                        "source_id": backup.data["id"],
                        **unpacked_ids
                    },
                    "$addToSet": {
                        "loaders": ctx.author.id
                    }
                },
                upsert=True
            )
        finally:
            await self.client.redis.publish("loaders:done", msgpack.packb({
                "id": ctx.guild_id,
                "type": "template",
                "source_id": str(backup.data["id"]),
                "template_id": name.strip("/").split("/")[-1]
            }))
            await ctx.bot.create_audit_log(utils.AuditLogType.TEMPLATE_LOAD, [ctx.guild_id], ctx.author.id)

    @template.command(aliases=("ls", "search", "s"))
    @wkr.cooldown(1, 10)
    async def list(self, ctx, *, search):
        """
        Get a list of the available templates, you should also check out the [templates](https://templates.xenon.bot) site.


        __Examples__

        All templates: ```{b.prefix}template list```
        Search: ```{b.prefix}template search roleplay```
        """
        menu = TemplateListMenu(ctx, search)
        await menu.start()

    @template.command(aliases=("i",))
    @wkr.cooldown(5, 30)
    async def info(self, ctx, name):
        """
        Get information about a template


        __Arguments__

        **name**: The id of the backup or the guild id to for latest automated backup


        __Examples__

        ```{b.prefix}template info starter```
        """
        template = await ctx.client.mongo.dtpl.templates.find_one({
            "internal": True,
            "$or": [{"name": name}, {"_id": name}]
        })
        if template is None:
            template = await self._crossload_template(name)

        if template is None:
            raise ctx.f.ERROR(f"There is **no template** with the name `{name}`.")

        raise ctx.f.DEFAULT(embed=self._template_info(template))

    def _template_info(self, template):
        guild = wkr.Guild(template["data"])

        channels = utils.channel_tree(guild.channels)
        if len(channels) > 1024:
            channels = channels[:1000] + "\n...\n```"

        roles = "```{}```".format("\n".join([
            r.name for r in sorted(guild.roles, key=lambda r: r.position, reverse=True)
        ]))
        if len(roles) > 1024:
            roles = roles[:1000] + "\n...\n```"

        return {
            "title": template["name"] + (
                "  ✅" if template["approved"] else " ❌"
            ),
            "description": template["description"],
            "fields": [
                {
                    "name": "Creator",
                    "value": f"<@{template['creator_id']}>",
                    "inline": True
                },
                {
                    "name": "Uses",
                    "value": str(template["usage_count"]),
                    "inline": False
                },
                {
                    "name": "Channels",
                    "value": channels,
                    "inline": True
                },
                {
                    "name": "Roles",
                    "value": roles,
                    "inline": True
                }
            ]
        }

import xenon_worker as wkr
from enum import IntEnum
import pymongo

import utils


class AuditLogType(IntEnum):
    BACKUP_CREATE = 0
    BACKUP_LOAD = 1
    TEMPLATE_LOAD = 2
    COPY = 3
    CHATLOG_CREATE = 4
    CHATLOG_LOAD = 5
    MESSAGE_SYNC_CREATE = 6
    BAN_SYNC_CREATE = 7
    SYNC_DELETE = 8


text_formats = {
    AuditLogType.BACKUP_CREATE: "<@{user}> created a backup of this server",
    AuditLogType.BACKUP_LOAD: "<@{user}> loaded a backup on this server",
    AuditLogType.TEMPLATE_LOAD: "<@{user}> loaded a template on this server",
    AuditLogType.COPY: "<@{user}> copied the server with the id `{source}` to the server with the id `{target}`",
    AuditLogType.CHATLOG_CREATE: "<@{user}> created a chatlog of the channel <#{channel}>",
    AuditLogType.CHATLOG_LOAD: "<@{user}> loaded a chatlog in the channel <#{channel}>",
    AuditLogType.MESSAGE_SYNC_CREATE: "<@{user}> created a message sync from <#{source}> to "
                                      "<#{target}> with the id `{id}`",
    AuditLogType.BAN_SYNC_CREATE: "<@{user}> created a ban sync from the server with the id `{source}` to "
                                  "the server with the id `{target}` with the id `{id}`",
    AuditLogType.SYNC_DELETE: "<@{user}> deleted a sync with the id `{id}`"
}


class AuditLogList(wkr.ListMenu):
    embed_kwargs = {"title": "Audit Logs"}

    async def get_items(self):
        args = {
            "limit": 10,
            "skip": self.page * 10,
            "sort": [("timestamp", pymongo.DESCENDING)],
            "filter": {
                "guilds": self.ctx.guild_id,
            }
        }
        logs = self.ctx.bot.db.audit_logs.find(**args)
        items = []
        async for audit_log in logs:
            items.append((
                utils.datetime_to_string(audit_log["timestamp"]),
                text_formats[AuditLogType(audit_log[type])].format(**audit_log, **audit_log["extra"])
            ))

        return items


class AuditLogs(wkr.Module):
    @wkr.Module.listener()
    async def on_load(self, *_, **__):
        await self.bot.db.audit_logs.create_index([("timestamp", pymongo.ASCENDING)])
        await self.bot.db.audit_logs.create_index([("user", pymongo.ASCENDING)])
        await self.bot.db.audit_logs.create_index([("guild", pymongo.ASCENDING)])

    @wkr.Module.command(name="audit logs", aliases=("logs",))
    @wkr.guild_only
    @wkr.has_permissions(administrator=True)
    @wkr.cooldown(1, 10, bucket=wkr.CooldownType.GUILD)
    async def audit_logs(self, ctx):
        """
        Get a list of actions that were recently taken on this guild
        (backup create, backup load, template load, copy from, copy to,
        chatlog create, chatlog load, sync create, sync delete)


        __Examples__

        ```{b.prefix}backup list```
        """
        menu = AuditLogList(ctx)
        await menu.start()

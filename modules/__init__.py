from .help import Help
from .admin import Admin
from .backups import Backups
from .templates import Templates
from .basics import Basics
from .redis import Redis
from .blacklist import Blacklist
from .premium import Premium
from .audit_logs import AuditLogs
from .settings import Settings


to_load = (Help, Admin, Backups, Basics, Templates, Redis, Blacklist, Premium, AuditLogs, Settings)

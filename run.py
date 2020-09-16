from bot import Xenon
from os import environ as env


bot = Xenon(
    prefix=env.get("PREFIX") or "#!",
    mongo_url=env.get("MONGO_URL") or "mongodb://localhost",
    rabbit_url=env.get("RABBIT_URL") or "amqp://guest:guest@localhost/",
    redis_url=env.get("REDIS_URL") or "redis://localhost/",
    redis_db=int(env.get("REDIS_DB") or "0")
)
bot.run(token=env.get("TOKEN"), shared_queue="main")

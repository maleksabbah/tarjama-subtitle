"""
Subtitle Service Redis Client
Pop from queue:subtitle, push to queue:completed.
"""
import json
import redis.asyncio as redis
from app.Config import config


client: redis.Redis = None


async def init_redis():
    global client
    client = redis.from_url(config.REDIS_URL, decode_responses=True)


async def close_redis():
    global client
    if client:
        await client.close()


async def pop_subtitle_task(timeout: int = 5) -> dict | None:
    """Pop a task from the subtitle queue."""
    result = await client.brpop(config.QUEUE_SUBTITLE, timeout=timeout)
    if result:
        _, data = result
        return json.loads(data)
    return None


async def push_completed(message: dict):
    """Push completion message to queue:completed."""
    await client.lpush(config.QUEUE_COMPLETED, json.dumps(message))
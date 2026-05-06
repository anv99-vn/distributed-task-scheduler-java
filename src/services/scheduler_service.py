"""
Scheduler Service — Steps 6 & 7
Polls Redis every SCHEDULER_POLL_MS milliseconds.
Uses a Lua script to atomically pop due tasks, then publishes
them to RabbitMQ sharded by user_id.
"""

import asyncio
import json
import logging
import time
from pathlib import Path

import aio_pika
from sqlalchemy import select

from src.config import settings
from src.database.postgres import AsyncSessionFactory
from src.database.rabbitmq import EXCHANGE_NAME, get_channel, setup_topology
from src.database.redis_client import ZSET_KEY, get_redis
from src.models.task import Task

logger = logging.getLogger(__name__)

LUA_SCRIPT = (Path(__file__).parent.parent / "scripts" / "lua" / "atomic_pop.lua").read_text()


async def _load_script(redis) -> str:
    return await redis.script_load(LUA_SCRIPT)


async def _pop_due_tasks(redis, sha: str) -> list[str]:
    """Return flat list [id, score, id, score, ...] for tasks due now."""
    now = str(int(time.time()))
    result = await redis.evalsha(sha, 1, ZSET_KEY, now, str(settings.scheduler_batch_size))
    return result or []


async def _publish_task(channel: aio_pika.Channel, task: Task) -> None:
    exchange = await channel.get_exchange(EXCHANGE_NAME)
    # Shard by user_id to avoid noisy-neighbour problem
    routing_key = f"tasks.user.{task.user_id}"
    body = json.dumps({
        "task_id": str(task.id),
        "user_id": task.user_id,
        "webhook_url": task.webhook_url,
        "payload": task.payload,
        "max_retries": task.max_retries,
        "retry_count": task.retry_count,
    }).encode()
    await exchange.publish(
        aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        ),
        routing_key=routing_key,
    )


async def run_scheduler_loop() -> None:
    logger.info("Scheduler started (poll=%dms)", settings.scheduler_poll_ms)
    await setup_topology()
    redis = await get_redis()
    sha = await _load_script(redis)
    interval = settings.scheduler_poll_ms / 1000.0

    while True:
        try:
            raw = await _pop_due_tasks(redis, sha)
            if raw:
                # raw = [id, score, id, score, ...]
                task_ids = raw[0::2]
                async with AsyncSessionFactory() as db:
                    result = await db.execute(
                        select(Task).where(Task.id.in_(task_ids))
                    )
                    tasks = result.scalars().all()

                channel = await get_channel()
                for task in tasks:
                    await _publish_task(channel, task)
                    logger.info("Scheduler: enqueued task %s (user=%s)", task.id, task.user_id)
        except Exception:
            logger.exception("Scheduler cycle error")
        await asyncio.sleep(interval)

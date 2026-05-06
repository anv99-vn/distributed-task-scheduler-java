"""
Relay Service — Step 5
Scans PostgreSQL every RELAY_INTERVAL_SECONDS for tasks due within
RELAY_LOOKAHEAD_MINUTES and pushes their IDs into the Redis ZSET.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update

from src.config import settings
from src.database.postgres import AsyncSessionFactory
from src.database.redis_client import ZSET_KEY, get_redis
from src.models.task import Task, TaskStatus

logger = logging.getLogger(__name__)


async def relay_once() -> int:
    deadline = datetime.now(timezone.utc) + timedelta(minutes=settings.relay_lookahead_minutes)

    async with AsyncSessionFactory() as db:
        result = await db.execute(
            select(Task).where(
                Task.status == TaskStatus.PENDING,
                Task.next_run_at <= deadline,
            )
        )
        tasks = result.scalars().all()

        if not tasks:
            return 0

        redis = await get_redis()
        mapping: dict[str, float] = {
            str(t.id): t.next_run_at.timestamp()  # type: ignore[union-attr]
            for t in tasks
        }
        await redis.zadd(ZSET_KEY, mapping)

        task_ids = [t.id for t in tasks]
        await db.execute(
            update(Task)
            .where(Task.id.in_(task_ids))
            .values(status=TaskStatus.SCHEDULED, updated_at=datetime.now(timezone.utc))
        )
        await db.commit()

    logger.info("Relay: pushed %d tasks to Redis", len(tasks))
    return len(tasks)


async def run_relay_loop() -> None:
    logger.info("Relay service started (interval=%ds)", settings.relay_interval_seconds)
    while True:
        try:
            count = await relay_once()
            if count:
                logger.info("Relay cycle done: %d tasks scheduled", count)
        except Exception:
            logger.exception("Relay cycle failed")
        await asyncio.sleep(settings.relay_interval_seconds)

"""
Worker Service — Steps 8 & 9
Consumes from RabbitMQ, executes webhooks, writes results to PostgreSQL
(with Optimistic Locking) and logs to Cassandra.
"""

import asyncio
import json
import logging
import socket
import uuid
from datetime import datetime, timezone

import aio_pika
import httpx
from sqlalchemy import select, update

from src.config import settings
from src.database.cassandra_client import get_session
from src.database.postgres import AsyncSessionFactory
from src.database.rabbitmq import EXCHANGE_NAME, QUEUE_PREFIX, get_connection, setup_topology
from src.models.task import Task, TaskStatus

logger = logging.getLogger(__name__)
WORKER_ID = socket.gethostname()


async def _update_status_optimistic(
    task_id: str, from_status: str, to_status: str, version: int, **extra
) -> bool:
    """Returns True if update succeeded (version matched), False on conflict."""
    async with AsyncSessionFactory() as db:
        result = await db.execute(
            update(Task)
            .where(
                Task.id == uuid.UUID(task_id),
                Task.status == from_status,
                Task.version == version,
            )
            .values(
                status=to_status,
                version=version + 1,
                updated_at=datetime.now(timezone.utc),
                **extra,
            )
            .returning(Task.id)
        )
        updated = result.fetchone()
        await db.commit()
        return updated is not None


def _log_execution(
    task_id: str,
    exec_id: uuid.UUID,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    result: str,
    error_msg: str,
) -> None:
    try:
        session = get_session()
        session.execute(
            """
            INSERT INTO task_execution_logs
                (task_id, exec_id, started_at, finished_at, status, result, error_msg, worker_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (task_id, exec_id, started_at, finished_at, str(status), result, error_msg, WORKER_ID),
        )
    except Exception:
        logger.exception("Failed to write Cassandra log for task %s", task_id)


async def _handle_message(message: aio_pika.IncomingMessage) -> None:
    async with message.process(requeue=False):
        body = json.loads(message.body)
        task_id: str = body["task_id"]
        exec_id = uuid.uuid4()
        started_at = datetime.now(timezone.utc)

        # Fetch current version for optimistic locking
        async with AsyncSessionFactory() as db:
            row = await db.execute(select(Task.version, Task.status).where(Task.id == uuid.UUID(task_id)))
            rec = row.one_or_none()

        if rec is None:
            logger.warning("Task %s not found in DB, dropping message", task_id)
            return

        current_version, current_status = rec
        if current_status not in (TaskStatus.SCHEDULED, TaskStatus.PENDING):
            logger.info("Task %s already in status %s, skipping", task_id, current_status)
            return

        # Step 8a: mark RUNNING with optimistic lock
        acquired = await _update_status_optimistic(
            task_id, current_status, TaskStatus.RUNNING, current_version
        )
        if not acquired:
            logger.warning("Optimistic lock failed for task %s (concurrent worker?), dropping", task_id)
            return

        # Step 8b: execute webhook
        status = TaskStatus.SUCCESS
        result_text = ""
        error_text = ""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(body["webhook_url"], json=body["payload"])
                resp.raise_for_status()
                result_text = resp.text[:4096]
                logger.info("Task %s succeeded (HTTP %d)", task_id, resp.status_code)
        except Exception as exc:
            status = TaskStatus.FAILED
            error_text = str(exc)[:2048]
            logger.error("Task %s failed: %s", task_id, error_text)

        finished_at = datetime.now(timezone.utc)

        # Compute next_run for recurring tasks
        next_run = None
        if status == TaskStatus.SUCCESS and body.get("cron_expression"):
            from croniter import croniter
            next_run = croniter(body["cron_expression"], finished_at).get_next(datetime)

        # Step 9a: update PostgreSQL
        extra: dict = {}
        if next_run:
            extra["next_run_at"] = next_run
            extra["status"] = TaskStatus.PENDING
        elif status == TaskStatus.FAILED and body["retry_count"] < body["max_retries"]:
            extra["retry_count"] = body["retry_count"] + 1
            extra["status"] = TaskStatus.PENDING
        else:
            extra["status"] = status

        await _update_status_optimistic(
            task_id, TaskStatus.RUNNING, extra.pop("status"), current_version + 1, **extra
        )

        # Step 9b: write audit log to Cassandra (sync, runs in thread pool)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            _log_execution,
            task_id, exec_id, started_at, finished_at,
            status, result_text, error_text,
        )


async def run_worker(user_shard: str = "#") -> None:
    """
    user_shard: routing key pattern, e.g. "tasks.user.alice" or "tasks.user.#"
    """
    await setup_topology()
    connection = await get_connection()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    queue_name = f"{QUEUE_PREFIX}{user_shard}"
    queue = await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "tasks.dlx",
            "x-message-ttl": 86_400_000,  # 24h TTL
        },
    )
    exchange = await channel.get_exchange(EXCHANGE_NAME)
    await queue.bind(exchange, routing_key=f"tasks.user.{user_shard}")

    logger.info("Worker started, consuming from %s", queue_name)
    await queue.consume(_handle_message)

    await asyncio.Future()  # run forever

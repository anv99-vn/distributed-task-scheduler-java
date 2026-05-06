import uuid
from datetime import datetime, timedelta, timezone

from croniter import croniter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.database.redis_client import ZSET_KEY, get_redis
from src.models.task import Task, TaskStatus
from src.schemas.task import TaskCreate


def _compute_next_run(task_create: TaskCreate) -> datetime:
    now = datetime.now(timezone.utc)
    if task_create.cron_expression:
        return croniter(task_create.cron_expression, now).get_next(datetime)
    return task_create.execute_at  # type: ignore[return-value]


async def create_task(db: AsyncSession, data: TaskCreate) -> Task:
    next_run = _compute_next_run(data)
    task = Task(
        user_id=data.user_id,
        name=data.name,
        description=data.description,
        cron_expression=data.cron_expression,
        execute_at=data.execute_at,
        next_run_at=next_run,
        webhook_url=str(data.webhook_url),
        payload=data.payload,
        max_retries=data.max_retries,
        status=TaskStatus.PENDING,
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)

    # Step 4: buffer into Redis if due within the lookahead window
    lookahead = datetime.now(timezone.utc) + timedelta(minutes=settings.relay_lookahead_minutes)
    if next_run <= lookahead:
        redis = await get_redis()
        await redis.zadd(ZSET_KEY, {str(task.id): next_run.timestamp()})
        task.status = TaskStatus.SCHEDULED
        await db.commit()

    return task


async def get_task(db: AsyncSession, task_id: uuid.UUID) -> Task | None:
    result = await db.execute(select(Task).where(Task.id == task_id))
    return result.scalar_one_or_none()


async def list_tasks(
    db: AsyncSession, user_id: str | None, page: int, page_size: int
) -> tuple[list[Task], int]:
    query = select(Task)
    count_query = select(func.count()).select_from(Task)
    if user_id:
        query = query.where(Task.user_id == user_id)
        count_query = count_query.where(Task.user_id == user_id)
    query = query.offset((page - 1) * page_size).limit(page_size).order_by(Task.created_at.desc())
    tasks = (await db.execute(query)).scalars().all()
    total = (await db.execute(count_query)).scalar_one()
    return list(tasks), total


async def cancel_task(db: AsyncSession, task_id: uuid.UUID) -> Task | None:
    """
    Returns the task if cancel succeeded.
    Returns None if task not found.
    Raises ValueError if task exists but is not cancellable.
    """
    task = await get_task(db, task_id)
    if task is None:
        return None
    if task.status not in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
        raise ValueError(f"Cannot cancel task in status {task.status}")

    from sqlalchemy import update as sa_update
    result = await db.execute(
        sa_update(Task)
        .where(
            Task.id == task_id,
            Task.status.in_([TaskStatus.PENDING, TaskStatus.SCHEDULED]),
        )
        .values(status=TaskStatus.CANCELLED, updated_at=datetime.now(timezone.utc))
        .returning(Task.id)
    )
    await db.commit()

    if result.fetchone() is None:
        raise ValueError(f"Cannot cancel task in status {task.status}")

    redis = await get_redis()
    await redis.zrem(ZSET_KEY, str(task_id))

    await db.refresh(task)
    return task

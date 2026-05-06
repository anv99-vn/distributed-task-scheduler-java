import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.postgres import get_db
from src.schemas.task import TaskCreate, TaskListResponse, TaskResponse
from src.services.task_service import cancel_task, create_task, get_task, list_tasks  # get_task used in GET endpoint

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.post("", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def submit_task(
    request: Request,
    data: TaskCreate,
    db: AsyncSession = Depends(get_db),
):
    task = await create_task(db, data)
    return task


@router.get("", response_model=TaskListResponse)
async def list_tasks_endpoint(
    db: AsyncSession = Depends(get_db),
    user_id: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    tasks, total = await list_tasks(db, user_id, page, page_size)
    return TaskListResponse(tasks=tasks, total=total, page=page, page_size=page_size)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task_endpoint(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    task = await get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.delete("/{task_id}", response_model=TaskResponse)
async def cancel_task_endpoint(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    try:
        task = await cancel_task(db, task_id)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

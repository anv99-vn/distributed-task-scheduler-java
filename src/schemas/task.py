import uuid
from datetime import datetime
from typing import Any

from croniter import croniter
from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


class TaskCreate(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=128)
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    cron_expression: str | None = None
    execute_at: datetime | None = None
    webhook_url: HttpUrl
    payload: dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0, le=10)

    @field_validator("cron_expression")
    @classmethod
    def validate_cron(cls, v: str | None) -> str | None:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: {v!r}")
        return v

    @model_validator(mode="after")
    def validate_schedule(self) -> "TaskCreate":
        if self.cron_expression is None and self.execute_at is None:
            raise ValueError("Either cron_expression or execute_at must be provided")
        if self.cron_expression is not None and self.execute_at is not None:
            raise ValueError("Provide only one of cron_expression or execute_at")
        return self


class TaskResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    user_id: str
    name: str
    description: str | None
    cron_expression: str | None
    execute_at: datetime | None
    next_run_at: datetime | None
    webhook_url: str
    payload: dict[str, Any]
    status: str
    version: int
    max_retries: int
    retry_count: int
    created_at: datetime
    updated_at: datetime


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]
    total: int
    page: int
    page_size: int

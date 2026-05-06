import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from src.api.routes.tasks import router as task_router
from src.config import settings
from src.database.postgres import init_db
from src.database.rabbitmq import close_rabbitmq, setup_topology
from src.database.redis_client import close_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

limiter = Limiter(key_func=get_remote_address, default_limits=[f"{settings.rate_limit_per_minute}/minute"])

app = FastAPI(title="Distributed Task Scheduler", version="1.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.on_event("startup")
async def startup():
    await init_db()
    await setup_topology()
    logging.getLogger(__name__).info("API Gateway ready")


@app.on_event("shutdown")
async def shutdown():
    await close_redis()
    await close_rabbitmq()


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logging.getLogger(__name__).error(
        "Unhandled exception on %s %s:\n%s",
        request.method, request.url.path,
        traceback.format_exc(),
    )
    return JSONResponse(status_code=500, content={"detail": str(exc), "type": type(exc).__name__})


app.include_router(task_router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok"}

"""Entry point: Scheduler (Redis ZSET → RabbitMQ via Lua atomic pop)"""
import asyncio
import logging

from src.services.scheduler_service import run_scheduler_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

if __name__ == "__main__":
    asyncio.run(run_scheduler_loop())

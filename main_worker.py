"""Entry point: Worker (RabbitMQ consumer → webhook executor → PostgreSQL + Cassandra)"""
import asyncio
import logging
import sys

from src.services.worker_service import run_worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

if __name__ == "__main__":
    # Optionally pass a user shard pattern as CLI arg, e.g. "user123" or "#" for all
    shard = sys.argv[1] if len(sys.argv) > 1 else "#"
    asyncio.run(run_worker(shard))

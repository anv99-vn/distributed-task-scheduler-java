"""Entry point: Relay Service (PostgreSQL → Redis ZSET)"""
import asyncio
import logging

from src.database.postgres import init_db
from src.services.relay_service import run_relay_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

if __name__ == "__main__":
    async def main():
        await init_db()
        await run_relay_loop()

    asyncio.run(main())

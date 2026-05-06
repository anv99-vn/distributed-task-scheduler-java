import aio_pika
from aio_pika import Channel, Connection, ExchangeType

from src.config import settings

EXCHANGE_NAME = "task_scheduler"
QUEUE_PREFIX = "tasks.user."
DLQ_NAME = "tasks.dead_letter"

_connection: Connection | None = None
_channel: Channel | None = None


async def get_connection() -> Connection:
    global _connection
    if _connection is None or _connection.is_closed:
        _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    return _connection


async def get_channel() -> Channel:
    global _channel
    if _channel is None or _channel.is_closed:
        conn = await get_connection()
        _channel = await conn.channel()
        await _channel.set_qos(prefetch_count=10)
    return _channel


async def setup_topology() -> None:
    channel = await get_channel()

    # Dead-letter exchange
    dlx = await channel.declare_exchange("tasks.dlx", ExchangeType.FANOUT, durable=True)
    dlq = await channel.declare_queue(DLQ_NAME, durable=True)
    await dlq.bind(dlx)

    # Main topic exchange (sharding by user_id)
    await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)


async def close_rabbitmq() -> None:
    global _connection, _channel
    if _channel and not _channel.is_closed:
        await _channel.close()
    if _connection and not _connection.is_closed:
        await _connection.close()
    _connection = None
    _channel = None

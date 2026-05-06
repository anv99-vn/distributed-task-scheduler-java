import logging
import time

from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy

from src.config import settings

logger = logging.getLogger(__name__)

_session: Session | None = None

_MAX_RETRIES = 10
_RETRY_DELAY = 10  # seconds


def get_session() -> Session:
    global _session
    if _session is None or _session.is_shutdown:
        _session = _connect_with_retry()
    return _session


def _connect_with_retry() -> Session:
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            cluster = Cluster(
                contact_points=settings.cassandra_host_list,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                protocol_version=4,
                connect_timeout=15,
            )
            session = cluster.connect()
            _ensure_schema(session)
            logger.info("Cassandra connected on attempt %d", attempt)
            return session
        except Exception as e:
            logger.warning(
                "Cassandra connection attempt %d/%d failed: %s. Retrying in %ds...",
                attempt, _MAX_RETRIES, e, _RETRY_DELAY,
            )
            time.sleep(_RETRY_DELAY)
    raise RuntimeError(f"Cannot connect to Cassandra after {_MAX_RETRIES} attempts")


def _ensure_schema(session: Session) -> None:
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {settings.cassandra_keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(settings.cassandra_keyspace)
    session.execute("""
        CREATE TABLE IF NOT EXISTS task_execution_logs (
            task_id     text,
            exec_id     uuid,
            started_at  timestamp,
            finished_at timestamp,
            status      text,
            result      text,
            error_msg   text,
            worker_id   text,
            PRIMARY KEY (task_id, exec_id)
        ) WITH CLUSTERING ORDER BY (exec_id DESC)
          AND default_time_to_live = 7776000
    """)


def close_cassandra() -> None:
    global _session
    if _session and not _session.is_shutdown:
        _session.cluster.shutdown()
    _session = None

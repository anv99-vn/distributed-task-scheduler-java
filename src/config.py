from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/taskscheduler"
    redis_url: str = "redis://localhost:6379/0"
    rabbitmq_url: str = "amqp://admin:admin@localhost:5672/"
    cassandra_hosts: str = "localhost"
    cassandra_keyspace: str = "task_scheduler"

    relay_interval_seconds: int = 300
    relay_lookahead_minutes: int = 15
    scheduler_poll_ms: int = 100
    scheduler_batch_size: int = 50

    rate_limit_per_minute: int = 60

    @property
    def cassandra_host_list(self) -> list[str]:
        return [h.strip() for h in self.cassandra_hosts.split(",")]


settings = Settings()

from dataclasses import dataclass
from urllib.parse import quote


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    sslmode: str = "prefer"

    @property
    def dsn(self) -> str:
        # psycopg/asyncpg compatible DSN
        user = quote(self.username, safe="")
        pwd = quote(self.password, safe="")
        db = quote(self.database, safe="")
        return f"postgresql://{user}:{pwd}@{self.host}:{self.port}/{db}?sslmode={self.sslmode}"


POSTGRES_CONFIG = PostgresConfig(
    # NOTE:
    # - Inside Docker network: host is usually "pgvector"
    # - From Windows host (outside Docker): use Docker host IP or localhost (because ports: "5432:5432")
    host="192.168.1.66",
    port=5432,
    database="postgres",
    username="n8n",
    password="n8n",
    sslmode="prefer",
)


def load_postgres_config() -> PostgresConfig:
    """Static config only (no .env, no env overrides)."""
    return POSTGRES_CONFIG


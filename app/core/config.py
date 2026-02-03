from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)

    PROJECT_NAME: str = "WQ Manager API"
    VERSION: str = "1.0.0"
    DESCRIPTION: str = "WQ Manager Backend API"
    API_PREFIX: str = "/api/v1"

    # Security
    SECRET_KEY: str = "change-me"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7
    ALGORITHM: str = "HS256"

    # CORS
    ALLOWED_ORIGINS: List[str] = [
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    ]

    # Database
    DATABASE_URL: str = ""
    DEBUG: bool = True

    # Logging
    LOG_LEVEL: str = "INFO"
    SQL_ECHO: bool = True
    SQL_ECHO_POOL: bool = False
    LOG_DIR: str = "logs"

    # Redis Cache
    REDIS_URL: str = ""
    CACHE_EXPIRE_HOUR: int = 14
    CACHE_EXPIRE_MINUTE: int = 0
    CACHE_TIMEZONE: str = "Asia/Shanghai"

    @property
    def ASYNC_DATABASE_URL(self) -> str:
        if self.DATABASE_URL.startswith("mysql+pymysql://"):
            return self.DATABASE_URL.replace("mysql+pymysql://", "mysql+aiomysql://", 1)
        if self.DATABASE_URL.startswith("mysql://"):
            return self.DATABASE_URL.replace("mysql://", "mysql+aiomysql://", 1)
        return self.DATABASE_URL


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

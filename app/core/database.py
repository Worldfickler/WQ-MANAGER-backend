import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from app.core.config import settings

# Configure SQLAlchemy logging
if settings.SQL_ECHO:
    logging.basicConfig()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    if settings.SQL_ECHO_POOL:
        logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)

engine = create_async_engine(
    settings.ASYNC_DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=settings.SQL_ECHO,
    echo_pool=settings.SQL_ECHO_POOL,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


async def get_db() -> AsyncSession:
    """Async database dependency for FastAPI."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_session() -> AsyncSession:
    """Create an AsyncSession for background tasks."""
    return AsyncSessionLocal()

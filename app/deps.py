from contextlib import asynccontextmanager
from fastapi import HTTPException
from fastapi import Header
from config import settings
import asyncpg

asyncpg_pool = None

async def init_asyncpg_pool():
    global asyncpg_pool
    asyncpg_pool = await asyncpg.create_pool(
        settings.db_dsn,
        min_size=1,
        max_size=20
    )

async def close_asyncpg_pool():
    global asyncpg_pool
    if asyncpg_pool:
        await asyncpg_pool.close()

@asynccontextmanager
async def get_async_conn():
    async with asyncpg_pool.acquire() as conn:
        yield conn

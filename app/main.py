from fastapi import FastAPI
from deps import init_asyncpg_pool, close_asyncpg_pool, get_async_conn
from contextlib import asynccontextmanager
from config import settings
from pydantic import BaseModel, Field
from uuid import UUID
from datetime import date
from typing import List, Literal
import asyncpg
from asyncpg.exceptions import UniqueViolationError
import json

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 起動時の処理
    await init_asyncpg_pool()
    yield
    # 終了時の処理
    await close_asyncpg_pool()

if settings.env == "development":
    app = FastAPI(lifespan=lifespan)
else:
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)

class Meta(BaseModel):
    report_id: UUID
    app: str = Field(min_length=1)
    language: str = Field(min_length=2, max_length=8)
    country: str = Field(min_length=2, max_length=2)
    date_bucket: date
    os: Literal["iOS", "iPadOS", "macOS"]
    os_major: int = Field(ge=0)
    os_minor: int = Field(ge=0)
    app_major: int = Field(ge=0)
    app_minor: int = Field(ge=0)
    app_patch: int = Field(ge=0)

class Event(BaseModel):
    name: str = Field(min_length=1)
    count: int = Field(ge=0)

class EventReport(BaseModel):
    meta: Meta
    events: List[Event]

class EventReportsRequest(BaseModel):
    reports: List[EventReport]

@app.post("/collect")
async def collect(request: EventReportsRequest):
    inserted = 0
    duplicates = 0

    async with get_async_conn() as conn:
        for report in request.reports:
            payload_json = json.dumps(
                report.model_dump(),
                ensure_ascii=False,
                default=str,
            )

            try:
                await conn.execute(
                    """
                    INSERT INTO raw_reports (
                        app,
                        report_id,
                        date_bucket,
                        payload
                    )
                    VALUES ($1, $2, $3, $4::jsonb)
                    """,
                    report.meta.app,
                    report.meta.report_id,
                    report.meta.date_bucket,
                    payload_json,
                )
                inserted += 1

            except UniqueViolationError:
                # 冪等：再送・二重送信は無視
                duplicates += 1

    return {
        "status": "ok",
        "inserted": inserted,
        "duplicates": duplicates,
    }
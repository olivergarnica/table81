import os
from datetime import date, timedelta
from typing import Sequence
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Integer, BigInteger, Float, Date, TIMESTAMP,
    PrimaryKeyConstraint, select, func
)
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Environment
load_dotenv()

DB_USER = os.getenv("user")
DB_PASS = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = os.getenv("port", "5432")
DB_NAME = os.getenv("dbname")

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    raise RuntimeError("Missing one of required DB env vars: user/password/host/dbname")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
)

# NullPool: each script run opens its own connections and closes them (good for Prefect tasks)
engine = create_engine(DATABASE_URL, poolclass=NullPool, future=True)
metadata = MetaData()


# Table Defn

channels = Table(
    "channels", metadata,
    Column("channel_id", String, primary_key=True),
    Column("title", String, nullable=False),
    Column("subscribers", Integer),
    Column("total_views", BigInteger),
    Column("created_at", Date),  # channel creation/published date
    Column("last_updated_at", TIMESTAMP, server_default=func.now(), onupdate=func.now())
)

videos = Table(
    "videos", metadata,
    Column("video_id", String, primary_key=True),
    Column("channel_id", String, nullable=False),
    Column("title", String),
    Column("description", String),
    Column("tags", String),
    Column("thumbnail_type", String),
    Column("published_at", TIMESTAMP),
    Column("last_updated_at", TIMESTAMP, server_default=func.now(), onupdate=func.now())
    # You can add ForeignKey('channels.channel_id') later if you want strict FK enforcement
)

channel_daily_stats = Table(
    "channel_daily_stats", metadata,
    Column("channel_id", String, nullable=False),
    Column("date", Date, nullable=False),
    Column("views", Integer),
    Column("subs_gained", Integer),
    Column("subs_lost", Integer),
    Column("estimated_minutes_watched", Float),
    Column("ingested_at", TIMESTAMP, server_default=func.now()),
    PrimaryKeyConstraint("channel_id", "date")
)

video_daily_stats = Table(
    "video_daily_stats", metadata,
    Column("video_id", String, nullable=False),
    Column("date", Date, nullable=False),
    Column("views", Integer),
    Column("likes", Integer),
    Column("comments", Integer),
    Column("shares", Integer),
    Column("watch_time", Float), 
    Column("avg_view_duration", Float),
    Column("avg_view_percent", Float),
    Column("ingested_at", TIMESTAMP, server_default=func.now()),
    PrimaryKeyConstraint("video_id", "date")
)

video_monthly_stats = Table(
    "video_monthly_stats", metadata,
    Column("video_id", String, nullable=False),
    Column("month", Date, nullable=False), 
    Column("views", BigInteger),
    Column("likes", BigInteger),
    Column("comments", BigInteger),
    Column("shares", BigInteger),
    Column("watch_time", Float),
    Column("avg_view_duration", Float),
    Column("avg_view_percent", Float),
    Column("rolled_up_at", TIMESTAMP, server_default=func.now()),
    PrimaryKeyConstraint("video_id", "month")
)

def create_tables():
    """Create all tables if they do not already exist."""
    metadata.create_all(engine)

"""
Bulk upsert rows into table using Postgres ON CONFLICT.
conflict_cols must match a unique index or primary key (here: PK columns).
"""
def _upsert(table: Table, rows: Sequence[dict], conflict_cols: Sequence[str]) -> int:
    if not rows:
        return 0

    stmt = pg_insert(table).values(list(rows))

    excluded = {col.name: col for col in stmt.excluded}

    update_map = {
        col.name: excluded[col.name]
        for col in table.columns
        if col.name not in conflict_cols   
        and col.name in excluded           
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=list(conflict_cols),
        set_=update_map
    )

    with engine.begin() as conn:
        conn.execute(stmt)

    return len(rows)

# Convenience Wrappersh
def upsert_channels(rows: Sequence[dict]):          return _upsert(channels, rows, ["channel_id"])
def upsert_videos(rows: Sequence[dict]):            return _upsert(videos, rows, ["video_id"])
def upsert_channel_daily(rows: Sequence[dict]):     return _upsert(channel_daily_stats, rows, ["channel_id", "date"])
def upsert_video_daily(rows: Sequence[dict]):       return _upsert(video_daily_stats, rows, ["video_id", "date"])
def upsert_video_monthly(rows: Sequence[dict]):     return _upsert(video_monthly_stats, rows, ["video_id", "month"])


"""
Delete rows older than `retain_days` from video_daily_stats.
Returns number of rows deleted (rowcount may be -1 if unknown).
"""
def prune_old_video_daily(retain_days: int = 30) -> int:
    cutoff = date.today() - timedelta(days=retain_days)
    with engine.begin() as conn:
        result = conn.execute(
            video_daily_stats.delete().where(video_daily_stats.c.date < cutoff)
        )
        return result.rowcount or 0

"""
Aggregate daily rows older than `retain_days` into monthly rows,
then delete those daily rows.  Idempotent via upsert.
"""
def rollup_video_daily_to_monthly(retain_days: int = 30) -> None:
    cutoff = date.today() - timedelta(days=retain_days)
    month_expr = func.date_trunc("month", video_daily_stats.c.date).cast(Date)

    agg_stmt = (
        select(
            video_daily_stats.c.video_id.label("video_id"),
            month_expr.label("month"),
            func.sum(video_daily_stats.c.views).label("views"),
            func.sum(video_daily_stats.c.likes).label("likes"),
            func.sum(video_daily_stats.c.comments).label("comments"),
            func.sum(video_daily_stats.c.shares).label("shares"),
            func.sum(video_daily_stats.c.watch_time).label("watch_time"),
            func.avg(video_daily_stats.c.avg_view_duration).label("avg_view_duration"),
            func.avg(video_daily_stats.c.avg_view_percent).label("avg_view_percent"),
        )
        .where(video_daily_stats.c.date < cutoff)
        .group_by(video_daily_stats.c.video_id, month_expr)
    )

    with engine.begin() as conn:
        rows = [dict(r._mapping) for r in conn.execute(agg_stmt)]
        if rows:
            upsert_video_monthly(rows)
            conn.execute(
                video_daily_stats.delete().where(video_daily_stats.c.date < cutoff)
            )

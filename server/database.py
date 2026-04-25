import os
import json
import asyncpg
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://taskuser:taskpass@localhost:5432/taskdb")

pool = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id     TEXT PRIMARY KEY,
                task_type   TEXT NOT NULL,
                payload     JSONB,
                priority    INTEGER DEFAULT 1,
                status      TEXT DEFAULT 'queued',
                worker_id   TEXT,
                created_at  TIMESTAMP DEFAULT NOW(),
                started_at  TIMESTAMP,
                completed_at TIMESTAMP,
                error       TEXT,
                retry_count INTEGER DEFAULT 3
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)
        """)
    print("Database initialized")


async def save_task(task_data: dict):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tasks (task_id, task_type, payload, priority, status, created_at, retry_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
            task_data["task_id"],
            task_data["task_type"],
            json.dumps(task_data["payload"]),
            task_data["priority"],
            task_data["status"],
            datetime.fromisoformat(task_data["created_at"]),
            task_data["retry_count"],
        )


async def get_task(task_id: str):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tasks WHERE task_id = $1", task_id)
        if row:
            return dict(row)
    return None


async def update_task_status(task_id: str, status: str, worker_id: str, error: str = None):
    async with pool.acquire() as conn:
        if status == "processing":
            await conn.execute("""
                UPDATE tasks SET status=$1, worker_id=$2, started_at=NOW()
                WHERE task_id=$3
            """, status, worker_id, task_id)
        elif status == "completed":
            await conn.execute("""
                UPDATE tasks SET status=$1, completed_at=NOW()
                WHERE task_id=$2
            """, status, task_id)
        elif status == "failed":
            await conn.execute("""
                UPDATE tasks SET status=$1, error=$2
                WHERE task_id=$3
            """, status, error, task_id)
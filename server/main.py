import os
import json
import uuid
import asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Any
import aio_pika
import redis.asyncio as aioredis
from database import init_db, save_task, get_task

app = FastAPI(title="Distributed Task Queue")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

redis_client = None
rabbitmq_connection = None
rabbitmq_channel = None


class TaskRequest(BaseModel):
    task_type: str          # "email", "image_resize", "data_process"
    payload: dict
    priority: Optional[int] = 1   # 1=low, 2=medium, 3=high
    retry_count: Optional[int] = 3


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


@app.on_event("startup")
async def startup():
    global redis_client, rabbitmq_connection, rabbitmq_channel
    # Connect Redis
    redis_client = aioredis.from_url(REDIS_URL)
    # Connect RabbitMQ
    rabbitmq_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    rabbitmq_channel = await rabbitmq_connection.channel()
    # Declare queues per priority
    await rabbitmq_channel.declare_queue("tasks.high", durable=True)
    await rabbitmq_channel.declare_queue("tasks.medium", durable=True)
    await rabbitmq_channel.declare_queue("tasks.low", durable=True)
    # Init DB
    await init_db()
    print("Server started successfully")


@app.on_event("shutdown")
async def shutdown():
    await rabbitmq_connection.close()
    await redis_client.close()


@app.post("/tasks", response_model=TaskResponse)
async def submit_task(task: TaskRequest):
    task_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat()

    task_data = {
        "task_id": task_id,
        "task_type": task.task_type,
        "payload": task.payload,
        "priority": task.priority,
        "retry_count": task.retry_count,
        "status": "queued",
        "created_at": created_at
    }

    # Save to PostgreSQL
    await save_task(task_data)

    # Cache status in Redis (TTL 1 hour)
    await redis_client.setex(
        f"task:{task_id}",
        3600,
        json.dumps({"status": "queued", "created_at": created_at})
    )

    # Route to priority queue
    queue_name = {3: "tasks.high", 2: "tasks.medium", 1: "tasks.low"}.get(task.priority, "tasks.low")

    await rabbitmq_channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(task_data).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # survive broker restart
        ),
        routing_key=queue_name
    )

    return TaskResponse(task_id=task_id, status="queued", message=f"Task queued in {queue_name}")


@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    # Check Redis cache first
    cached = await redis_client.get(f"task:{task_id}")
    if cached:
        data = json.loads(cached)
        data["task_id"] = task_id
        data["source"] = "cache"
        return data

    # Fallback to PostgreSQL
    task = await get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Re-cache
    await redis_client.setex(f"task:{task_id}", 3600, json.dumps(task))
    task["source"] = "database"
    return task


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/stats")
async def stats():
    # Queue depths
    conn = await aio_pika.connect_robust(RABBITMQ_URL)
    ch = await conn.channel()
    high = await ch.declare_queue("tasks.high", durable=True, passive=True)
    medium = await ch.declare_queue("tasks.medium", durable=True, passive=True)
    low = await ch.declare_queue("tasks.low", durable=True, passive=True)
    await conn.close()

    return {
        "queues": {
            "high": high.declaration_result.message_count,
            "medium": medium.declaration_result.message_count,
            "low": low.declaration_result.message_count,
        },
        "timestamp": datetime.utcnow().isoformat()
    }

import os
import json
import asyncio
import asyncpg
import redis.asyncio as aioredis
import aio_pika
from datetime import datetime

WORKER_ID = os.getenv("WORKER_ID", "worker1")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://taskuser:taskpass@localhost:5432/taskdb")

redis_client = None
db_pool = None
tasks_processed = 0
tasks_failed = 0


async def process_task(task_data: dict) -> bool:
    """Simulate task processing based on task_type"""
    task_type = task_data.get("task_type")
    payload = task_data.get("payload", {})

    print(f"[{WORKER_ID}] Processing {task_type} task: {task_data['task_id'][:8]}...")

    try:
        if task_type == "email":
            # Simulate sending email
            await asyncio.sleep(0.05)
            print(f"[{WORKER_ID}] Email sent to {payload.get('to', 'unknown')}")

        elif task_type == "image_resize":
            # Simulate image processing
            await asyncio.sleep(0.1)
            print(f"[{WORKER_ID}] Image resized: {payload.get('filename', 'unknown')}")

        elif task_type == "data_process":
            # Simulate data processing
            await asyncio.sleep(0.02)
            print(f"[{WORKER_ID}] Data processed: {payload.get('records', 0)} records")

        else:
            # Generic task
            await asyncio.sleep(0.01)

        return True

    except Exception as e:
        print(f"[{WORKER_ID}] Task failed: {e}")
        return False


async def update_status(task_id: str, status: str, error: str = None):
    """Update task status in Redis and PostgreSQL"""
    # Update Redis cache
    await redis_client.setex(
        f"task:{task_id}",
        3600,
        json.dumps({
            "status": status,
            "worker_id": WORKER_ID,
            "updated_at": datetime.utcnow().isoformat(),
            "error": error
        })
    )

    # Update PostgreSQL
    async with db_pool.acquire() as conn:
        if status == "processing":
            await conn.execute("""
                UPDATE tasks SET status=$1, worker_id=$2, started_at=NOW()
                WHERE task_id=$3
            """, status, WORKER_ID, task_id)
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


async def handle_message(message: aio_pika.IncomingMessage):
    global tasks_processed, tasks_failed

    async with message.process(requeue=True):
        task_data = json.loads(message.body.decode())
        task_id = task_data["task_id"]

        # Mark as processing
        await update_status(task_id, "processing")

        # Process the task
        success = await process_task(task_data)

        if success:
            await update_status(task_id, "completed")
            tasks_processed += 1
            print(f"[{WORKER_ID}] ✓ Completed {task_id[:8]} | Total: {tasks_processed}")
        else:
            retry_count = task_data.get("retry_count", 0)
            if retry_count > 0:
                # Requeue with decremented retry
                task_data["retry_count"] = retry_count - 1
                print(f"[{WORKER_ID}] Retrying task {task_id[:8]} ({retry_count} retries left)")
                # Message will be requeued via requeue=True
            else:
                await update_status(task_id, "failed", "Max retries exceeded")
                tasks_failed += 1


async def main():
    global redis_client, db_pool

    print(f"[{WORKER_ID}] Starting worker...")

    # Connect to Redis
    redis_client = aioredis.from_url(REDIS_URL)

    # Connect to PostgreSQL
    db_pool = await asyncpg.create_pool(DATABASE_URL)

    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    # Set prefetch - each worker handles 10 tasks at a time
    await channel.set_qos(prefetch_count=10)

    # Subscribe to all priority queues
    high_queue = await channel.declare_queue("tasks.high", durable=True)
    medium_queue = await channel.declare_queue("tasks.medium", durable=True)
    low_queue = await channel.declare_queue("tasks.low", durable=True)

    await high_queue.consume(handle_message)
    await medium_queue.consume(handle_message)
    await low_queue.consume(handle_message)

    print(f"[{WORKER_ID}] Listening on all queues (high/medium/low)...")

    # Keep worker alive
    try:
        await asyncio.Future()
    finally:
        await connection.close()
        await db_pool.close()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())

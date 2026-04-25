# Distributed Task Queue

A production-ready distributed task queue system built with Python, RabbitMQ, Redis, and PostgreSQL.

## Architecture

```
Client → FastAPI Server → RabbitMQ (3 priority queues)
                       ↓
              Worker1 | Worker2 | Worker3
                       ↓
              Redis (cache) + PostgreSQL (persistence)
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| API Server | FastAPI | REST API, task submission |
| Message Broker | RabbitMQ | Task queue, priority routing |
| Cache | Redis | Status cache, sub-ms lookups |
| Database | PostgreSQL | Persistent task storage |
| Workers | Python asyncio | Task processing (3 nodes) |
| Container | Docker Compose | Service orchestration |

## Features

- **Priority queues** — high/medium/low routing
- **At-least-once delivery** — persistent messages survive broker restart
- **Auto retry** — configurable retry count per task
- **Redis caching** — fast status lookups without DB hits
- **3 worker nodes** — parallel task processing
- **Health checks** — all services monitored

## Quick Start

### Prerequisites
- Docker Desktop installed
- Python 3.11+

### 1. Clone and Start

```bash
git clone <your-repo>
cd distributed-task-queue
docker-compose up --build
```

### 2. Submit a Task

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "email",
    "payload": {"to": "user@example.com"},
    "priority": 3
  }'
```

### 3. Check Task Status

```bash
curl http://localhost:8000/tasks/<task_id>
```

### 4. View Queue Stats

```bash
curl http://localhost:8000/stats
```

### 5. Run Benchmarks (get your resume metrics)

```bash
cd benchmarks
pip install -r requirements.txt
python benchmark.py
```

## Project Structure

```
distributed-task-queue/
├── docker-compose.yml
├── server/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py          # FastAPI app, task submission
│   └── database.py      # PostgreSQL layer
├── worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── worker.py        # RabbitMQ consumer
└── benchmarks/
    ├── requirements.txt
    └── benchmark.py     # Get real metrics
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /tasks | Submit a task |
| GET | /tasks/{id} | Get task status |
| GET | /stats | Queue depths |
| GET | /health | Health check |



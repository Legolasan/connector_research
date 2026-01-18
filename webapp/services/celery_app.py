"""
🔧 Celery Application Configuration
Distributed task queue for parallel research generation.
"""

import os
from celery import Celery

from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create Celery app
celery_app = Celery(
    "connector_research",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["webapp.services.tasks"]
)

# Configure Celery
celery_app.conf.update(
    # Serialization
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    
    # Task tracking
    task_track_started=True,
    result_extended=True,
    
    # Time limits
    task_time_limit=300,  # 5 min max per task
    task_soft_time_limit=270,  # Soft limit at 4.5 min
    
    # Concurrency
    worker_concurrency=4,  # 4 parallel workers per process
    worker_prefetch_multiplier=2,  # Prefetch 2 tasks per worker
    
    # Reliability
    task_acks_late=True,  # Ack after task completes (not before)
    task_reject_on_worker_lost=True,  # Reject task if worker dies
    
    # Result expiration
    result_expires=3600,  # Results expire after 1 hour
    
    # Task routing (optional, for future scaling)
    task_routes={
        "webapp.services.tasks.web_search_task": {"queue": "search"},
        "webapp.services.tasks.fetch_source_task": {"queue": "fetch"},
        "webapp.services.tasks.summarize_task": {"queue": "summarize"},
        "webapp.services.tasks.synthesis_supervisor_task": {"queue": "synthesis"},
    },
    
    # Default queue
    task_default_queue="default",
)


def get_celery_app() -> Celery:
    """Get the Celery app instance."""
    return celery_app

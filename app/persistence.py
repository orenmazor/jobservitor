"""Not a real persistence layer, just a thin wrapper around our redis enqueue/dequeue logic for now.

Purpose is just to keep the rest of the code clean and consistent"""

import redis
from os import environ
from typing import Optional, Literal, Dict

PROJECT_PREFIX = "jobservitor:"
QUEUE_PREFIX = "jobservitor:queue:"

redis_client = redis.from_url(
    environ.get("REDIS_URI", "redis://localhost:6379/0"), decode_responses=True
)


def save_job(job) -> bool:
    return redis_client.set(PROJECT_PREFIX + job.id, job.model_dump_json())


def load_job(job_id) -> str | None:
    return redis_client.get(PROJECT_PREFIX + job_id)


def enqueue_job(job) -> bool:
    # score by submission timestamp so we can FIFO as much as possible
    score = job.submitted_at.timestamp()

    return redis_client.zadd(QUEUE_PREFIX + job.gpu_type, {job.id: score})


def dequeue_job(
    gpu_type: Literal["NVIDIA", "AMD", "Intel", "Any"] = "Any",
    blocking_time: int = 1,
) -> Optional[Dict]:
    # this needs to be abstracted out. this service shouldn't know about redis
    queued_work = redis_client.bzpopmin(QUEUE_PREFIX + gpu_type, timeout=blocking_time)

    # TODO: we have to check the job requirements in addition to the gpu type!

    return queued_work

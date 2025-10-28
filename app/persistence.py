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

print(
    f"Connected to Redis at {environ.get('REDIS_URI', 'redis://localhost:6379/0')}, version {redis_client.info()['redis_version']}"
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
    gpu_type: Literal["NVIDIA", "AMD", "Intel", "Any"],
    cpu_cores: int,
    memory_gb: int,
    blocking_time: int = 1,
    dc: str = "Any",
    region: str = "Any",
) -> Optional[Dict]:
    """
    Get the next job that should be worked on, given the passed in requirements
    TODO: we have to check the job requirements in addition to the gpu type!
    we got two options here, we can switch from a bzpopmin to a zrange, which pulls
    my objects, and then i can do ZREM once i find the job that matches my requirements best
    but that's not properly atomic
    an alternative is using a lua script to do the same thing atomically
    another alternative is creating a semaphore on this dequeue job function
    to make sure that only one executor is dequeing at a time, but then
    we're introducing a whole new bottleneck

    lua is the way to go probably, but lets do the "bad" way first to get something working,
    and revisit the lua way once I have enough tests to test the refactor with
    """
    # avoid circular imports
    # the fact we're here means I'm breaking my own layeringn rules
    # which means that this entire function does not belong here
    # this should be either refactored to move into the executor logic, the model logic,
    # or if I switch to lua, entirely done-away with
    from app.models import Job

    # first version of this is just a bzpopmin, but it can only get one item
    # queued_work = redis_client.bzpopmin(QUEUE_PREFIX + gpu_type, timeout=blocking_time)

    # so switch to zpopmin which can pop multiple items
    possible_jobs = redis_client.zpopmin(QUEUE_PREFIX + gpu_type, count=10)
    if not possible_jobs:
        # we found none, so return none and let the caller try again
        return None

    # TODO: need to wrap all of this in a try/catch because if ANYTHING goes wrong in this function
    # we will lose jobs from the queue
    selected_job = None
    for queued_work in possible_jobs:
        job = Job.model_validate_json(load_job(queued_work[0]))

        # this is wildly inefficient
        # because we waste a lookup  on jobs we dont want even after we found
        # the selected job
        # BUT if this is all being replaced with lua, this temporary code
        # is fine to leave as is until tthis whole logic is luafied
        if (
            selected_job is None
            and job.memory_requested <= memory_gb
            and job.cpu_cores_requested <= cpu_cores
        ):
            selected_job = job
        else:
            enqueue_job(job)  # put it back in the queue

    return selected_job

from typing import List, Optional, Literal, Dict
from datetime import datetime
from uuid import uuid4
import redis
from os import environ

from pydantic import BaseModel

redis_client = redis.from_url(
    environ.get("REDIS_URI", "redis://localhost:6379/0"), decode_responses=True
)


class JobCreate(BaseModel):
    """
    Separate the job creation model from the runtime model.
    This is a cleaner way to make sure users can't modify the housekeeping
    fields like id/status/timestamps.

    The alternatives, while still using pydantic, could be a pre-save hook
    that validates that the user is not trying to set these fields.
    but that's less readable I think?
    It also might make serialization/deserialization less readable,
    at which point I'd need to not use pydantic and go with a heavier
    tool before I needed to..
    """

    image: str
    command: List[str]
    arguments: List[str]
    gpu_type: Literal["Intel", "NVIDIA", "AMD"] = "NVIDIA"
    memory_requested: int = 1  # in GB
    cpu_cores_requested: int = 1


class Job(JobCreate):
    # job housekeeping stuff
    id: str = str(uuid4())
    status: Literal["pending", "running", "succeeded", "failed"] = "pending"
    submitted_at: datetime = datetime.now()
    aborted_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def save(self) -> bool:
        """
        pydantic isn't really an ORM but less is more.
        This persists the entire job object to redis so we can retrieve it later, and avoid
        saving the entire object in the redis queue.
        """
        return redis_client.set(f"jobservitor:{self.id}", self.model_dump_json())

    def enqueue(self) -> bool:
        """
        Push the job ID onto the redis queue.

        We're going to use a simple sorted set in redis for this for now that uses the submitted_at timestamp
        as the score
        """
        score = int(self.submitted_at.timestamp())
        return redis_client.zadd("jobservitor:queue", {self.id: score})

    @classmethod
    def load(cls, job_id) -> Optional["Job"]:
        """Dear oren. are you just reinventing ActiveRecord?"""
        data = redis_client.get(f"jobservitor:{job_id}")
        if data:
            return Job.model_validate_json(data)
        return None

    @classmethod
    def all(cls) -> List[Dict]:
        """Retrieve all jobs from redis. DEFINITELY just recreating AR now"""
        # TODO: this is a potentially expensive call and will need to be rewritten
        # TODO2: do not rehydrate the IDs here, force the client to do it
        values = redis_client.zrange("jobservitor:queue", 0, -1, withscores=True)

        # TODO3: is it better to rehydrate here or return these fragments? the problem here
        # is that because we only store fragments of the job definition in the sorted set
        # when we do this mini rehydration we are potentially setting incorrect defaults
        # return [
        #     Job(id=job_id, submitted_at=timestamp) for (job_id, timestamp) in values
        # ]
        # return a non-rehydrated dictionary instead for now and let upstream figure it out
        return [
            {"job_id": job_id, "submitted_at": datetime.fromtimestamp(timestamp)}
            for (job_id, timestamp) in values
        ]

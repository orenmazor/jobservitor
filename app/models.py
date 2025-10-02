from typing import List, Optional, Literal
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


class Job(BaseModel):
    # job housekeeping stuff
    id: str = str(uuid4())
    status: str = "pending"
    submitted_at: datetime = datetime.now()
    aborted_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def save(self):
        """
        pydantic isn't really an ORM but less is more.
        This persists the entire job object to redis so we can retrieve it later, and avoid
        saving the entire object in the redis queue.
        """
        return redis_client.set(f"jobservitor:{self.id}", self.model_dump_json())

    @classmethod
    def load(cls, job_id):
        """Dear oren. are you just reinventing ActiveRecord?"""
        data = redis_client.get(f"jobservitor:{job_id}")
        if data:
            return Job.model_validate_json(data)
        return None

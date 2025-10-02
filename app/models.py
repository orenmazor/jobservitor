from typing import List, Optional, Literal
from datetime import datetime
from uuid import uuid4
import redis
from os import environ

from pydantic import BaseModel, PrivateAttr

redis_client = redis.from_url(
    environ.get("REDIS_URI", "redis://localhost:6379/0"), decode_responses=True
)


# TODO: runtime is in redis but it would be dope to have a persistent layer in sqlite or something like that
class Job(BaseModel):
    # job runtime stuff
    # could be a separate object but why? we'll never see
    # jobs sharing definitions. probably?
    # either way, no optionals here
    # explicit is better than implicit
    image: str
    command: List[str]
    arguments: List[str]
    gpu_type: Literal["Intel", "NVIDIA", "AMD"] = "NVIDIA"
    memory_requested: int = 1  # in GB
    cpu_cores_requested: int = 1

    # job housekeeping stuff
    # forbid accepting
    _id: str = PrivateAttr(default_factory=lambda: str(uuid4()))
    _status: str = PrivateAttr(default="pending")
    _submitted_at: datetime = PrivateAttr(default_factory=datetime.now)
    _aborted_at: Optional[datetime] = PrivateAttr()
    _completed_at: Optional[datetime] = PrivateAttr()

    @property
    def id(self) -> str:
        return self._id

    @property
    def status(self) -> str:
        return self._status

    @property
    def submitted_at(self) -> datetime:
        return self._submitted_at

    @property
    def aborted_at(self) -> Optional[datetime]:
        return self._aborted_at

    @property
    def completed_at(self) -> Optional[datetime]:
        return self._completed_at

    def dict(self, *args, **kwargs):
        """Override dict to include private attributes"""
        base = super().dict(*args, **kwargs)
        base.update(
            {
                "id": self.id,
                "status": self.status,
                "submitted_at": self.submitted_at,
                "aborted_at": self.aborted_at,
                "completed_at": self.completed_at,
            }
        )
        return base

    def save(self):
        """pydantic isn't really an ORM but less is more"""
        redis_client.set("jobservitor:{self.id}", self.model_dump_json())

    def find(self):
        """pydantic isn't really an ORM but less is more"""
        data = redis_client.get(f"jobservitor:{self.id}")
        if data:
            return Job.model_validate_json(data)
        return None

    @classmethod
    def all(cls, value) -> List["Job"]:
        """Dear oren. are you just reinventing ActiveRecord?"""
        # TODO: get queue
        return []

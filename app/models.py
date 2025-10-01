from typing import List, Optional, Literal
from datetime import datetime
from uuid import uuid4

from pydantic import BaseModel, PrivateAttr


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

    # job housekeeping stuff
    # forbid accepting
    _id: str = PrivateAttr(default_factory=uuid4)
    _status: str = PrivateAttr(default="pending")
    _submitted_at: datetime = PrivateAttr(default_factory=datetime.now)
    _aborted_at: Optional[datetime] = PrivateAttr()
    _completed_at: Optional[datetime] = PrivateAttr()

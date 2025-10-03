# this is a roughed out executor service.
# it connects to redis and monitors a zset for jobs that it can do
# TODO: should it receive shutdown notices from the scheduler? or redis? does it matter?
# TODO: add responsible signal handling for graceful shutdown
from typing import Optional, Literal

from persistence import redis_client
from time import sleep
from os import environ
from models import Job

idle_time = environ.get("EXECUTOR_IDLE_TIME", 1)
blocking_time = environ.get("EXECUTOR_BLOCKING_TIME", 5)


def handle_one_job(
    gpu_type: Optional[Literal["NVIDIA", "ATI", "Intel"]] = None,
    cpu_cores: int = 1,
    memory_gb: int = 1,
) -> Optional[Job]:
    queued_work = redis_client.bzpopmin("jobservitor:queue", timeout=blocking_time)
    if queued_work is None:
        print("No job found, sleeping...")
        return

    job = Job.load(queued_work[1])

    return job


def listen_for_work(
    gpu_type: Optional[Literal["NVIDIA", "ATI", "Intel"]] = None,
    cpu_cores: int = 1,
    memory_gb: int = 1,
):

    while True:
        handle_one_job(gpu_type, cpu_cores, memory_gb)
        sleep(idle_time)


if __name__ == "__main__":
    # TODO: add discovery of these things, rather than relying on env input
    gpu_type = environ.get("EXECUTOR_GPU_TYPE", None)
    cpu_cores = int(environ.get("EXECUTOR_CPU_CORES", 1))
    memory_gb = int(environ.get("EXECUTOR_MEMORY_GB", 1))

    listen_for_work(gpu_type, cpu_cores, memory_gb)

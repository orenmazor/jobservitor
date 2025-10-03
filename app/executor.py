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
blocking_time = environ.get("EXECUTOR_BLOCKING_TIME", 1)


def _pop_job(
    gpu_type: Literal["NVIDIA", "ATI", "Intel", "Any"] = "Any"
) -> Optional[Job]:
    # this needs to be abstracted out. this service shouldn't know about redis
    queued_work = redis_client.bzpopmin(
        f"jobservitor:queue:{gpu_type}", timeout=blocking_time
    )

    # TODO: we have to check the job requirements in addition to the gpu type!

    return queued_work


# TODO: could be rewritten as a generator. for funsies and better readability.
def handle_one_job(
    gpu_type: Literal["NVIDIA", "ATI", "Intel", "Any"],
    cpu_cores: int,
    memory_gb: int,
) -> Optional[Job]:

    for queue_to_monitor in [gpu_type, "Any"]:
        queued_work = _pop_job(queue_to_monitor)
        if queued_work:
            break

    if queued_work is None:
        print("No job found, sleeping...")
        return

    job = Job.load(queued_work[1])
    # TODO: check that the job matches the executor's capabilities
    # TODO: should i have architecture based queues?

    # TODO: run job somehow

    return job


def listen_for_work(
    gpu_type: Literal["NVIDIA", "ATI", "Intel", "Any"] = "Any",
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

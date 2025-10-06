# this is a roughed out executor service.
# it connects to redis and monitors a zset for jobs that it can do
# TODO: should it receive shutdown notices from the scheduler? or redis? does it matter?
# TODO: add responsible signal handling for graceful shutdown
from typing import Optional, Literal

from persistence import dequeue_job
from time import sleep
from os import environ
from models import Job
from datetime import datetime
from socket import gethostname, gethostbyname

idle_time = environ.get("EXECUTOR_IDLE_TIME", 1)
blocking_time = environ.get("EXECUTOR_BLOCKING_TIME", 1)

# not the best way to get the IP
executor_name = (
    environ.get("EXECUTOR_NAME", "executor-1") + "-" + gethostbyname(gethostname())
)


# TODO: could be rewritten as a generator. for funsies and better readability.
def handle_one_job(
    gpu_type: Literal["NVIDIA", "AMD", "Intel", "Any"],
    cpu_cores: int,
    memory_gb: int,
) -> Optional[Job]:

    queued_work = dequeue_job(gpu_type, blocking_time)
    if queued_work is None:
        # check the ANY queue next"""
        queued_work = dequeue_job("Any", blocking_time)

    if queued_work is None:
        print("No job found, sleeping...")
        return

    job = Job.load(queued_work[1])

    if job.status != "pending":
        # this could be an exception at this layer, because a non-pending job
        # should never be popped
        return

    # TODO: check that the job matches the executor's capabilities
    # TODO: should i have architecture based queues?

    # TODO: run job somehow
    job.status = "running"
    job.started_at = datetime.now()
    job.worker = executor_name
    job.save()

    # simulate the job running by faking a docker execution?

    job.completed_at = datetime.now()
    job.status = "succeeded"
    job.save()

    return job


def listen_for_work(
    gpu_type: Literal["NVIDIA", "AMD", "Intel", "Any"] = "Any",
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

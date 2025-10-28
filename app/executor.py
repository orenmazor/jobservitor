# this is a roughed out executor service.
# it connects to redis and monitors a zset for jobs that it can do
# TODO: should it receive shutdown notices from the scheduler? or redis? does it matter?
# TODO: add responsible signal handling for graceful shutdown
from typing import Optional, Literal
import docker
from app.persistence import dequeue_job
from time import sleep
from os import environ
from sys import exit
from app.models import Job
from datetime import datetime
from socket import gethostname, gethostbyname

idle_time = environ.get("EXECUTOR_IDLE_TIME", 1)
blocking_time = environ.get("EXECUTOR_BLOCKING_TIME", 1)

try:
    client = docker.from_env()
    print("Docker server version: " + client.info()["ServerVersion"])
except docker.errors.DockerException as e:
    print(e)
    exit(1)

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

    job = dequeue_job(gpu_type, blocking_time, cpu_cores, memory_gb)
    if job is None:
        # check the ANY queue next"""
        job = dequeue_job("Any", blocking_time, cpu_cores, memory_gb)

    if job is None:
        print("No job found, sleeping...")
        return

    if job.status != "pending":
        # this could be an exception at this layer, because a non-pending job
        # should never be popped
        return

    job.status = "running"
    job.started_at = datetime.now()
    job.worker = executor_name
    job.save()

    # detach so that we can return to it and kill it if needed
    try:
        container = client.containers.run(
            image=job.image, command=" ".join(job.command + job.arguments), detach=True
        )
    except (docker.errors.ImageNotFound, docker.errors.APIError):
        job.status = "failed"
        job.completed_at = datetime.now()
        job.save()

        return job

    while container.status != "exited":
        # TODO: watch for kill signal on the executor
        # TODO: watch for resource consumption
        container.reload()

        # again,t his is very very bad engineering
        # and solely first implementation just to get this working
        # but what i need is a comms channel for the scheduler to tell the executor
        # to kill the job
        job = Job.load(job.id)
        if job.status == "aborted":
            container.kill()
            job.completed_at = datetime.now()
            job.status = "aborted"
            job.save()
            return job

    # massively not ideal, but properly managing these logs
    # is out of scope here (and indeed, for some enterprise tools that will
    # remain nameless..)
    print(container.logs().decode())
    job.completed_at = datetime.now()
    if container.wait()["StatusCode"] != 0:
        job.status = "failed"
    else:
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


def start_worker():
    """The entry point for the worker to configure itself
    and begin listening for work.
    """

    # TODO: add discovery of these things, rather than relying on env input
    gpu_type = environ.get("EXECUTOR_GPU_TYPE", "Any")
    cpu_cores = int(environ.get("EXECUTOR_CPU_CORES", 1))
    memory_gb = int(environ.get("EXECUTOR_MEMORY_GB", 1))

    listen_for_work(gpu_type=gpu_type, cpu_cores=cpu_cores, memory_gb=memory_gb)


if __name__ == "__main__":
    start_worker()

from fastapi.testclient import TestClient
import threading

from app.executor import handle_one_job

from app.scheduler import app
from app.models import Job, redis_client

from time import sleep

client = TestClient(app)


def test_no_job_found():
    assert handle_one_job(gpu_type="Any", cpu_cores=1, memory_gb=1) is None


def test_job_found():
    job_data = {
        "image": "busybox",
        "command": ["uname"],
        "arguments": ["-a"],
        "gpu_type": "NVIDIA",
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    # queue should have a job
    assert redis_client.zrange("jobservitor:queue:NVIDIA", 0, -1) == [
        response.json()["id"]
    ]

    assert (
        handle_one_job(gpu_type="NVIDIA", cpu_cores=1, memory_gb=2).id
        == response.json()["id"]
    )

    # queue should be empty now
    assert redis_client.zrange("jobservitor:queue:NVIDIA", 0, -1) == []


def test_executor_ignores_architecture_that_doesnt_belong_to_it():
    job_data = {
        "image": "busybox",
        "command": ["uname"],
        "arguments": ["-a"],
        "gpu_type": "NVIDIA",
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    # queue should have a job
    assert redis_client.zrange("jobservitor:queue:NVIDIA", 0, -1) == [
        response.json()["id"]
    ]

    assert handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2) is None

    # queue should still have the job
    assert redis_client.zrange("jobservitor:queue:NVIDIA", 0, -1) == [
        response.json()["id"]
    ]


def test_executor_checks_the_any_queue_after_checking_its_own_arch():
    job_data = {
        "image": "busybox",
        "command": ["uname"],
        "arguments": ["-a"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    # this job went to the Any queue
    assert redis_client.zrange("jobservitor:queue:Any", 0, -1) == [
        response.json()["id"]
    ]

    assert (
        handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2).id
        == response.json()["id"]
    )

    # queue should be empty
    assert redis_client.zrange("jobservitor:queue:Any", 0, -1) == []


def test_executor_completes_a_job_and_correctly_updates_it():
    job_data = {
        "image": "busybox",
        "command": ["uname"],
        "arguments": ["-a"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    pending_job = Job.load(response.json()["id"])
    assert pending_job.status == "pending"
    assert pending_job.started_at is None
    assert pending_job.completed_at is None

    complete_job = handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2)
    assert complete_job.id == pending_job.id
    assert complete_job.status == "succeeded"
    assert complete_job.started_at is not None
    assert complete_job.completed_at is not None
    assert complete_job.worker == "executor-1-127.0.0.1"

    # queue should be empty
    assert redis_client.zrange("jobservitor:queue:Any", 0, -1) == []


def test_executor_pulls_two_jobs_in_sequence():
    job_data = {
        "image": "busybox:1.36",
        "command": ["uname"],
        "arguments": ["-a"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response_first = client.post("/jobs", json=job_data)
    assert response_first.status_code == 200

    sleep(2)  # ensure the second job has a later timestamp
    job_data = {
        "image": "busybox:1.37",
        "command": ["uname"],
        "arguments": ["-a"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response_second = client.post("/jobs", json=job_data)
    assert response_second.status_code == 200

    # make extra sure that we did not mess up this part becuase nothing else will work
    assert response_first.json()["id"] != response_second.json()["id"]

    complete_job_first = handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2)
    assert complete_job_first.image == "busybox:1.36"

    complete_job_second = handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2)
    assert complete_job_second.image == "busybox:1.37"


def test_executor_respects_job_status():
    job_data = {
        "image": "busybox",
        "command": ["uname"],
        "arguments": ["-a"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response_first = client.post("/jobs", json=job_data)
    assert response_first.status_code == 200

    job = Job.load(response_first.json()["id"])
    job.status = "running"
    job.save()

    complete_job_first = handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2)
    assert complete_job_first is None


def test_executor_finds_the_one_job_it_can_run():
    job_data = {
        "image": "busybox:1.36",
        "command": ["uname"],
        "arguments": ["-a"],
        "memory_requested": 10,
        "cpu_cores_requested": 1,
    }
    assert client.post("/jobs", json=job_data).status_code == 200
    assert client.post("/jobs", json=job_data).status_code == 200
    assert client.post("/jobs", json=job_data).status_code == 200
    assert client.post("/jobs", json=job_data).status_code == 200
    assert client.post("/jobs", json=job_data).status_code == 200

    job_data = {
        "image": "busybox:1.37",
        "command": ["uname"],
        "arguments": ["-a"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    assert client.post("/jobs", json=job_data).status_code == 200

    complete_job = handle_one_job(gpu_type="Any", cpu_cores=1, memory_gb=1)
    assert complete_job.image == "busybox:1.37"


def test_executor_respects_the_exit_code_of_the_job():
    # this job will succeed
    job_data = {
        "image": "busybox:1.37",
        "command": ["sh"],
        "arguments": ["-c", "'exit 0'"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    assert client.post("/jobs", json=job_data).status_code == 200
    complete_job = handle_one_job(gpu_type="Any", cpu_cores=1, memory_gb=1)
    assert complete_job.status == "succeeded"

    # this job will fail
    job_data = {
        "image": "busybox:1.37",
        "command": ["sh"],
        "arguments": ["-c", "'exit 1000'"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    assert client.post("/jobs", json=job_data).status_code == 200
    complete_job = handle_one_job(gpu_type="Any", cpu_cores=1, memory_gb=1)
    assert complete_job.status == "failed"

    # this job will definitely fail
    job_data = {
        "image": "i am not a real image lol",
        "command": ["sh"],
        "arguments": ["-c", "'exit 0'"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    assert client.post("/jobs", json=job_data).status_code == 200
    complete_job = handle_one_job(gpu_type="Any", cpu_cores=1, memory_gb=1)
    assert complete_job.status == "failed"


def test_long_running_job_status():
    job_data = {
        "image": "busybox:1.37",
        "command": ["sleep"],
        "arguments": ["5"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    # using asyncio for this might be more fun
    temp_thread = threading.Thread(target=handle_one_job, args=("Any", 1, 1))
    temp_thread.start()

    assert Job.load(response.json()["id"]).status == "running"

    temp_thread.join()
    assert Job.load(response.json()["id"]).status == "succeeded"

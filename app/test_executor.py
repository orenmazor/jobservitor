from fastapi.testclient import TestClient

from executor import handle_one_job

from scheduler import app
from models import Job, redis_client

from time import sleep

client = TestClient(app)


def test_no_job_found():
    assert handle_one_job(gpu_type="Any", cpu_cores=1, memory_gb=1) is None


def test_job_found():
    job_data = {
        "image": "python:3.8",
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "NVIDIA",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
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
        "image": "python:3.8",
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "NVIDIA",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
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
        "image": "python:3.8",
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "memory_requested": 1,
        "cpu_cores_requested": 2,
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
        "image": "python:3.8",
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
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
        "image": "first",
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
    }
    response_first = client.post("/jobs", json=job_data)
    assert response_first.status_code == 200

    sleep(2)  # ensure the second job has a later timestamp
    job_data = {
        "image": "second",
        "command": ["python"],
        "arguments": ["-c", "print('Goodbye, Cruel World!')"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
    }
    response_second = client.post("/jobs", json=job_data)
    assert response_second.status_code == 200

    # make extra sure that we did not mess up this part becuase nothing else will work
    assert response_first.json()["id"] != response_second.json()["id"]

    complete_job_first = handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2)
    assert complete_job_first.image == "first"

    complete_job_second = handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2)
    assert complete_job_second.image == "second"


def test_executor_respects_job_status():
    job_data = {
        "image": "first",
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "AMD",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
    }
    response_first = client.post("/jobs", json=job_data)
    assert response_first.status_code == 200

    job = Job.load(response_first.json()["id"])
    job.status = "running"
    job.save()

    complete_job_first = handle_one_job(gpu_type="AMD", cpu_cores=1, memory_gb=2)
    assert complete_job_first is None

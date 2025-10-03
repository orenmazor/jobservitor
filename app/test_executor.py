from fastapi.testclient import TestClient

from executor import handle_one_job

from scheduler import app
from models import redis_client

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

    assert handle_one_job(gpu_type="ATI", cpu_cores=1, memory_gb=2) is None

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
        handle_one_job(gpu_type="ATI", cpu_cores=1, memory_gb=2).id
        == response.json()["id"]
    )

    # queue should be empty
    assert redis_client.zrange("jobservitor:queue:Any", 0, -1) == []

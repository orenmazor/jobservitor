from fastapi.testclient import TestClient

from scheduler import app
import pytest
from uuid import uuid4
from models import redis_client

client = TestClient(app)


# happy path
def test_submit_job():
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
    # simple check for a uuid. not great. not terrible
    assert len(response.json()["id"]) == 36


def test_submitting_and_getting_job_back():
    job_data = {
        "image": uuid4().hex,
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "NVIDIA",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200
    job_id = response.json()["id"]

    list_response = client.get("/jobs")
    assert list_response.status_code == 200
    assert list_response.json()[0]["id"] == job_id
    assert list_response.json()[0]["image"] == job_data["image"]


def test_housekeeping_parameters_cannot_be_set_on_job_creation():
    job_data = {
        "image": "python:3.8",
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "NVIDIA",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
        # attempting to set housekeeping parameters should be ignored
        "id": "malicious_id",
        "status": "succeeded",
        "submitted_at": "2020-01-01T00:00:00",
        "aborted_at": "2020-01-01T00:00:00",
        "completed_at": "2020-01-01T00:00:00",
    }
    response = client.post("/jobs", json=job_data)
    # should we return a 4xx error instead on extra data?
    assert response.status_code == 200
    job_id = response.json()["id"]
    assert job_id != "malicious_id"

    list_response = client.get("/jobs")
    assert list_response.status_code == 200
    job = list_response.json()[0]

    # we should see sane defaults here
    # and not what i submitted above
    assert job["id"] == job_id
    assert job["status"] == "pending"
    assert job["aborted_at"] is None
    assert job["completed_at"] is None


def test_fetching_a_job_by_id():
    job_data = {
        "image": uuid4().hex,
        "command": ["python"],
        "arguments": ["-c", "print('Hello, World!')"],
        "gpu_type": "NVIDIA",
        "memory_requested": 1,
        "cpu_cores_requested": 2,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200
    job_id = response.json()["id"]

    # now get the job back out by ID and confirm
    get_response = client.get(f"/jobs/{job_id}")
    assert get_response.status_code == 200
    job = get_response.json()
    assert job["id"] == job_id
    assert job["image"] == job_data["image"]


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

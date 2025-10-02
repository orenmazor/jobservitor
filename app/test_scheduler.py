from fastapi.testclient import TestClient

from scheduler import app
import pytest
from uuid import uuid4
from models import redis_client

client = TestClient(app)


@pytest.fixture(autouse=True)
def run_around_tests():
    # Code that will run before your test, e.g. setting up a test database
    yield  # This is where the testing happens
    redis_client.flushdb()  # Clear the Redis database after each test


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
    breakpoint()

    assert list_response.json()[0]["id"] == job_id
    assert list_response.json()[0]["image"] == job_data["image"]


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

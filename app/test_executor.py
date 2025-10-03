from fastapi.testclient import TestClient

from executor import handle_one_job

from scheduler import app
import pytest
from uuid import uuid4
from models import redis_client

client = TestClient(app)


def test_no_job_found():
    assert handle_one_job() is None


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

    assert handle_one_job().id == response.json()["id"]

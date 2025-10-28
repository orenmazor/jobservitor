from fastapi.testclient import TestClient

import threading
from app.models import Job
from app.executor import handle_one_job
from app.scheduler import app
from uuid import uuid4
from time import sleep

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
        "region": "az1",
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200
    job_id = response.json()["id"]

    list_response = client.get("/jobs")
    assert list_response.status_code == 200
    assert list_response.json()[0]["id"] == job_id
    assert list_response.json()[0]["image"] == job_data["image"]
    assert list_response.json()[0]["region"] == job_data["region"]
    assert list_response.json()[0]["dc"] == "Any"


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


def test_abort_pending_job():
    # pending job should be abortable
    job_data = {
        "image": "busybox:1.37",
        "command": ["uname"],
        "arguments": ["-a"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    abort_response = client.delete(f"/jobs/{response.json()["id"]}")
    assert abort_response.status_code == 200
    assert Job.load(response.json()["id"]).status == "aborted"


def test_abort_succeeded_job():
    # succeeded job should NOT be abortable
    job_data = {
        "image": "busybox:1.37",
        "command": ["uname"],
        "arguments": ["-a"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    complete_job = handle_one_job(
        gpu_type="Any", cpu_cores=1, memory_gb=1, dc="us-east-1", region="az1"
    )
    assert complete_job.status == "succeeded"

    abort_response = client.delete(f"/jobs/{response.json()["id"]}")
    assert abort_response.status_code == 400
    assert Job.load(response.json()["id"]).status == "succeeded"


def test_abort_aborted_job():
    job_data = {
        "image": "busybox:1.37",
        "command": ["uname"],
        "arguments": ["-a"],
        "memory_requested": 1,
        "cpu_cores_requested": 1,
    }
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 200

    abort_response = client.delete(f"/jobs/{response.json()["id"]}")
    assert Job.load(response.json()["id"]).status == "aborted"

    abort_response = client.delete(f"/jobs/{response.json()["id"]}")
    assert abort_response.status_code == 400
    assert "Job already completed, cannot abort. sorry!" in abort_response.text


def test_abort_running_job():
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
    temp_thread = threading.Thread(
        target=handle_one_job, args=("Any", 1, 1, "us-east-1", "az1")
    )
    temp_thread.start()

    # rest our weary roboheads for a moment
    sleep(1)

    assert Job.load(response.json()["id"]).status == "running"
    abort_response = client.delete(f"/jobs/{response.json()["id"]}")
    assert abort_response.status_code == 200

    temp_thread.join()
    assert Job.load(response.json()["id"]).status == "aborted"

from fastapi.testclient import TestClient

from scheduler import app

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


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

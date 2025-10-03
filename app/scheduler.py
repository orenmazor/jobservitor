from typing import List, Dict
from fastapi import FastAPI, HTTPException

from models import Job, JobCreate, redis_client

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    print("🚀 App is starting up...")
    print(f"✅ Connected to Redis {redis_client.info()["redis_version"]}")


@app.on_event("shutdown")
async def shutdown_event():
    print("🛑 App is shutting down...")
    # TODO: if i wanted to make a proper persistence layer and not redis messiness, this is where we could clean up


@app.post("/jobs")
def submit_job(job_create: JobCreate) -> Dict:
    """If the job fails to validate, fastapi will raise a 422 error automatically."""
    # using separated Job and JobCreate to protect housekeeping fields
    job = Job.model_validate({**job_create.model_dump()})

    # TODO: if persistence fails to redis what do?
    if job.save():
        # we saved the job to redis
        # toss it into the queue
        job.enqueue()
        return {"id": job.id}

    raise HTTPException(status_code=500, detail="Failed to save job")


@app.get("/jobs/{job_id}")
def get_job(job_id) -> Job:
    # TODO: pull the job based on the given ID from redis
    # TODO: altho using redis as a repoistory of jobs is not the best option. maybe keep just IDs in redis
    # but the rest of the job information in something more persistent like sqlite
    return Job.load(job_id)


@app.get("/jobs")
def list_jobs() -> List[Job]:
    """List existing jobs in redis"""
    # TODO: this relies on queued jobs only. are there jobs that are not queued that will be missed? like finished jobs?
    return [Job.load(job["job_id"]) for job in Job.all()]


@app.delete("/jobs/{job_id}")
def abort_job(job_id) -> bool:
    """Receives a job id and aborts it if the job is in pending/running status"""
    # TODO: find the job in the queue and delete it from the queue if it's pending
    # TODO: how to communicate to executor that they need to abort the job? will simply setting the job status to aborted be sufficient for THIS project?
    return HTTPException(status_code=501, detail="Not implemented yet")


@app.get("/")
@app.get("/health")
def health_check() -> Dict:
    """Health check endpoint for probes."""
    # TODO: check that redis is up
    # TODO: check that there is at least one executor registered
    return {"status": "ok"}

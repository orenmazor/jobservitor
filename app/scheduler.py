# this is a fast api service with 4 http endpoints:
# 1. a POST endpoint to submit a json job definition
# 2. a GET endpoint to get a job definition based on a job id submitted as a path parameter
# 3. a GET endpoint that returns a json list of all job ids
# 4. a DELETE endpoint to abort a job that receives a job id based as a path parameter

from typing import List, Dict
from fastapi import FastAPI, HTTPException

from models import Job

app = FastAPI()


@app.post("/jobs")
def submit_job(job: Job) -> Dict:
    """If the job fails to validate, fastapi will raise a 422 error automatically."""
    # TODO: persist to redis
    return {"id": job.id}


@app.get("/jobs/{job_id}")
def get_job(job_id) -> Dict:
    # TODO: pull the job based on the given ID from redis
    # TODO: altho using redis as a repoistory of jobs is not the best option. maybe keep just IDs in redis
    # but the rest of the job information in something more persistent like sqlite
    return HTTPException(status_code=501, detail="Not implemented yet")


@app.get("/")
def list_jobs() -> List[Dict]:
    """List existing jobs in redis"""
    # TODO: pull job IDs from redis and return.
    # TODO: should we return just IDs or hydrate the job object fully? more load on redis
    return HTTPException(status_code=501, detail="Not implemented yet")


@app.delete("/jobs/{job_id}")
def abort_job(job_id) -> bool:
    """Receives a job id and aborts it if the job is in pending/running status"""
    return HTTPException(status_code=501, detail="Not implemented yet")


@app.get("/")
@app.get("/health")
def health_check() -> Dict:
    """Health check endpoint for probes."""
    # TODO: check that redis is up
    # TODO: check that there is at least one executor registered
    return {"status": "ok"}

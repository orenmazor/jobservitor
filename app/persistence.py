import redis
from os import environ

redis_client = redis.from_url(
    environ.get("REDIS_URI", "redis://localhost:6379/0"), decode_responses=True
)

import pytest

from persistence import redis_client


@pytest.fixture(autouse=True)
def run_around_tests():
    redis_client.flushdb()  # Clear the Redis database before each test, in case I randomly killed it
    yield
    redis_client.flushdb()  # Clear the Redis database after each test, because I will probably randomly kill it

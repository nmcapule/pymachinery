import json

from redis import Redis
from typing import Dict


class RedisBroker:
    client: Redis

    def __init__(self, broker_uri: str):
        self.client = Redis.from_url(broker_uri)

    def send_task(self, queue: str, task: Dict):
        self.client.rpush(queue, json.dumps(task))

    def retrieve_task(self, queue: str, timeout=0):
        response = self.client.blpop(queue, timeout)
        if response is None:
            return None
        _, task = response
        return json.loads(task)

import json
import uuid

from redis import Redis
from typing import Dict, List


class RedisBroker:
    client: Redis

    def __init__(self, broker_uri: str):
        self.client = Redis.from_url(broker_uri)

    def send_simple_task(self, queue: str, name: str, args: List):
        task = {
            "UUID": f"task_{uuid.uuid1()}",
            "Name": name,
            "RoutingKey": queue,
            "ETA": None,
            "GroupUUID": "",
            "GroupTaskCount": 0,
            "Args": args,
            "Headers": {},
            "Priority": 0,
            "Immutable": False,
            "RetryCount": 0,
            "RetryTimeout": 0,
            "OnSuccess": None,
            "OnError": None,
            "BrokerMessageGroupId": "",
            "SQSReceiptHandle": "",
            "StopTaskDeletionOnError": False,
            "IgnoreWhenTaskNotRegistered": False
        }
        self.send_task(queue, task)
        return task

    def send_task(self, queue: str, task: Dict):
        self.client.rpush(queue, json.dumps(task))

    def retrieve_task(self, queue: str, timeout=0):
        response = self.client.blpop(queue, timeout)
        if response is None:
            return None
        _, task = response
        return json.loads(task)

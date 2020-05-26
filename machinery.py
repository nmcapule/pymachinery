import json
import logging
import time

from typing import Dict, Tuple

from broker.redis import RedisBroker
from backend.mongodb import MongoDbBackend

DEFAULT_QUEUE = "default_queue"


class Value:
    json: Dict

    def __init__(self, vtype, value, name=None):
        self.json = {"Value": value, "Type": vtype}
        if name is not None:
            self.json["Name"] = name

    def from_dict(table: Dict):
        return Value(
            vtype=table.get("Type"),
            value=table.get("Value"),
            name=table.get("Name"),
        )

    def name(self):
        return self.json.get("Name")

    def value(self):
        return self.json.get("Value")

    def vtype(self):
        return self.json.get("Type")


class Machinery:

    working: bool = True

    broker: RedisBroker
    backend: MongoDbBackend
    queue: str

    workers: Dict

    def __init__(self, broker_uri, backend_uri, queue=DEFAULT_QUEUE):
        self.broker = RedisBroker(broker_uri)
        self.backend = MongoDbBackend(backend_uri)
        self.queue = queue
        self.workers = {}

    def register_worker(self, task_name: str, callback):
        self.workers[task_name] = callback

    def start(self, poll_secs=3):
        while self.working:
            task = self.broker.retrieve_task(self.queue, poll_secs)
            if task is None:
                continue

            task_name = task.get("Name")
            if task_name not in self.workers:
                logging.error(f"Unknown task {task_name} from {self.queue}")
                self.broker.send_task(self.queue, task)
                time.sleep(poll_secs)
                continue

            logging.info(f"Got task: {task.get('UUID')}")

            callback = self.workers.get(task_name)
            try:
                # Convert dictionaries parameters into Value.
                parameters = [
                    Value.from_dict(item) for item in task.get("Args")
                ]
                results = callback(*parameters)
                # Convert Value outputs into dictionaries.
                if isinstance(results, list) or isinstance(results, tuple):
                    dict_list = [item.json for item in results]
                else:
                    dict_list = [results.json]
                self._on_task_success(task, dict_list)
            except Exception as err:
                raise err
                # self._on_task_failure(task, str(err))

    def _on_task_success(self, task: Dict, results: Tuple):
        self.backend.save_state(task, results=results, state="SUCCESS")

        if task.get("OnSuccess") is not None:
            subtask = task.get("OnSuccess")
            subtask["Args"] = [result for result in results]
            queue = subtask.get("RoutingKey") or self.queue
            self.backend.save_state(subtask, state="PENDING")
            self.broker.send_task(queue, subtask)

        group_uuid = task.get("GroupUUID")
        group_task_count = task.get("GroupTaskCount")
        chord_callback = task.get("ChordCallback")

        if group_uuid is None or chord_callback is None:
            return
        if not self.backend.is_group_completed(group_uuid, group_task_count):
            return

        subtask = chord_callback
        subtask["Args"] = self.backend.load_group_results(group_uuid)
        queue = subtask.get("RoutingKey") or self.queue
        self.backend.save_state(subtask, state="PENDING")
        self.backend.update_group_meta(group_uuid, chord_triggered=True)
        self.broker.send_task(queue, subtask)

    def _on_task_failure(self, task: Dict, error: str):
        self.backend.save_state(task, error=error, state="FAILED")

        if task.get("OnError") is not None:
            subtask = task.get("OnError")
            subtask["Args"] = [{
                "Name": "error",
                "Type": "string",
                "Value": error,
            }]
            queue = subtask.get("RoutingKey") or self.queue
            self.backend.save_state(subtask, state="PENDING")
            self.broker.send_task(queue, subtask)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    def add_worker(a: Value, b: Value):
        return (Value("int64", a.value() + b.value()))

    def multiply_worker(a: Value, b: Value):
        return (Value("int64", a.value() * b.value()))

    def worker(name: Value, delayms: Value):
        return (
            Value("string", f"hello {name.value()}"),
            Value("int64", delayms),
        )

    machinery = Machinery(
        broker_uri="redis://:helloworld@localhost:6379",
        backend_uri="mongodb://mongo:moonbucks@localhost:27017/?authSource=admin",
        queue="machinery_tasks",
    )
    machinery.register_worker("add", add_worker)
    machinery.register_worker("multiply", multiply_worker)
    machinery.register_worker("hello", worker)
    machinery.start()

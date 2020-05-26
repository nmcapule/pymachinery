from typing import Dict
from pymongo import MongoClient


class MongoDbBackend:
    client: MongoClient

    def __init__(self, backend_uri: str):
        self.client = MongoClient(backend_uri)

    def save_state(self, task: Dict, results=[], error=None, state="PENDING"):
        updates = {
            "results": [{
                "type": item.get("Type"),
                "value": item.get("Value"),
            } for item in results],
            "task_name": task.get("Name"),
            "state": state,
        }
        if error is not None:
            updates["error"] = error

        self._tasks().update_one(
            {"_id": task.get("UUID")},
            {
                "$set": updates,
                "$currentDate": {
                    "created_at": {
                        "$type": "date"
                    },
                },
            },
            upsert=True,
        )

    def load_results(self, task_uuid: str):
        entry = self._tasks().find_one({"_id": task_uuid})
        results = entry.get("results")
        if results is None:
            return None
        return [{
            "Type": item.get("type"),
            "Value": item.get("value"),
        } for item in results]

    def load_group_results(self, group_uuid: str):
        group_meta = self._gcms().find_one({"_id": group_uuid})
        if group_meta is None:
            return False
        task_uuids = group_meta.get("task_uuids")
        results = [self.load_results(uuid) for uuid in task_uuids]
        return [item for sub in results for item in sub]

    def is_group_completed(self, group_uuid: str, task_count: int):
        group_meta = self._gcms().find_one({"_id": group_uuid})
        if group_meta is None:
            return False

        task_uuids = group_meta.get("task_uuids")
        count = self._tasks().count_documents({
            "_id": {
                "$in": task_uuids
            },
            "state": "SUCCESS",
        })
        return count == task_count

    def update_group_meta(self,
                          group_uuid: str,
                          chord_triggered=None,
                          lock=None):
        updates = {}
        if chord_triggered is not None:
            updates["chord_trigger"] = chord_triggered
        if lock is not None:
            updates["lock"] = lock
        self._gcms().update_one(
            {"_id": group_uuid},
            {"$set": updates},
        )

    def _gcms(self):
        return self.client.machinery.group_metas

    def _tasks(self):
        return self.client.machinery.tasks

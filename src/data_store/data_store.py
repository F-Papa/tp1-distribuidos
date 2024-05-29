import json
import logging
from typing import Any

class DataStore:
    def __init__(self):
        self.data = {}

    def num_items(self):
        return len(self.data)

    @classmethod
    def restore_or_init(cls, file_path: str) -> "DataStore":
        return cls()

    def get(self, key: Any):
        key = json.dumps(key)

        result = self.data.get(key)
        if result:
            return json.loads(result)

    def add(self, key: Any, value: Any):
        key = json.dumps(key)
        self.data[key] = json.dumps(value)

    def clear(self):
        self.data.clear()
    
    def commit_to_disk(self):
        pass
        # logging.info("Committing data to disk")
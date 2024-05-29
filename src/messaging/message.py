import json
import logging
from typing import Any


class Message:
    def __init__(self, content: dict):
        self.content = content
    
    def keys(self):
        return self.content.keys()

    def has_key(self, key: str) -> bool:
        return key in self.content

    def get(self, key: str) -> Any:
        return self.content.get(key)

    def marshal(self):
        return json.dumps(self.content)

    @classmethod
    def unmarshal(cls, marshalled_msg: str):
        return Message(json.loads(marshalled_msg))

    # Used by middleware to set metadata
    def with_id(self, delivery_id: int):
        self.delivery_id = delivery_id
        return self
    
    def from_queue(self, queue_name: str):
        self.queue_name = queue_name
        return self
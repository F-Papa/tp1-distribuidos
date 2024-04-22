import json
from typing import Any

class Message:
    def __init__(self, content: dict):
        self.content = content

    def has_key(self, key: str) -> bool:
        return key in self.content

    def get(self, key: str) -> Any:
        return self.content.get(key)

    def marshal(self):
        return json.dumps(self.content)
    
    @classmethod
    def unmarshal(cls, marshalled_msg: str):
        return Message(json.loads(marshalled_msg))
        

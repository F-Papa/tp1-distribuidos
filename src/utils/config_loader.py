import configparser
from os import environ
from typing import Any

class Configuration:
    def __init__(self, required: dict[str, Any], config: dict[str, Any]):
        self.required = required
        self.properties = {}
        for key, value_type in self.required.items():
            if key in config:
                self.properties[key] = value_type(config[key])

    def get(self, key) -> Any:
        value = self.properties.get(key)
        if value is None:
            raise ValueError(f"Invalid property: {key}")
        return value

    def update(self, key, value):
        if key not in self.required:
            raise ValueError(f"Invalid property: {key}")

        value_type = self.required[key]
        self.properties[key] = value_type(value)

    def validate(self):
        for key, value_type in self.required.items():
            if not isinstance(self.properties.get(key), value_type):
                raise ValueError(f"Missing or invalid property: {key}")

    def update_from_env(self):
        for key in self.required:
            value = environ.get(key)
            if value is not None:
                self.update(key, value)

    @classmethod
    def from_file(cls, required: dict[str, Any], path: str):
        config = configparser.ConfigParser()
        config.read(path)
        config_dict = {k.upper(): v for k, v in config["CONFIG"].items()}
        return Configuration(required=required, config=config_dict)

    @classmethod
    def from_env(cls, required: dict[str, Any], path: str):
        config = {k: environ.get(k) for k in required}
        return Configuration(required=required, config=config)

    def __str__(self) -> str:
        formatted = ", ".join([f"{k}={v}" for k, v in self.properties.items()])
        return f"FilterConfig({formatted})"

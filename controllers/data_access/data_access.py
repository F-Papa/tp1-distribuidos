import json
import logging
import os
from typing import Any, Union, Callable


class DataAccess:

    class DataEntry:
        def __init__(
            self, key_type: type, value_type: type, key: Any, value: Any
        ) -> None:
            if not isinstance(key, key_type):
                raise ValueError(f"Key must be of type {key_type}")
            if not isinstance(value, value_type):
                raise ValueError(f"Value must be of type {value_type}")

            self.key = key
            self.value = value

    KEY_VALUE_SEPARATOR = "%%%"

    def __init__(
        self,
        key_type: type,
        value_type: type,
        cache_vacants: int,
        n_partitions: int,
        file_prefix: str,
        merge_function: Union[Callable, None] = None,
    ) -> None:
        self.cache: dict[key_type, value_type] = dict()
        self.key_type = key_type
        self.value_type = value_type
        self.cache_vacants = cache_vacants
        self.cached_entries = 0
        self.entries_in_files = 0
        self.n_partitions = n_partitions
        self.file_prefix = file_prefix
        self.merge_function = merge_function

    def _pop_oldest_cache_entry(self) -> DataEntry:
        first_cache_key = list(self.cache.keys())[0]
        first_cache_value = self.cache.pop(first_cache_key)

        return self.DataEntry(
            self.key_type, self.value_type, first_cache_key, first_cache_value
        )

    def _partition_for_entry(self, entry: DataEntry) -> int:
        return hash(entry.key) % self.n_partitions

    def n_elements_in_cache(self) -> int:
        return self.cached_entries

    def n_elements_in_files(self) -> int:
        return self.entries_in_files

    def _write_oldest_to_disk(self):
        oldest_entry = self._pop_oldest_cache_entry()
        partition = self._partition_for_entry(oldest_entry)
        file_name = f"{self.file_prefix}_{partition}.json"
        entry = f"{oldest_entry.key}{self.KEY_VALUE_SEPARATOR}{oldest_entry.value}\n"
        with open(file_name, "a") as f:
            f.write(entry)

        self.entries_in_files += 1
        self.cached_entries -= 1

    def _pop_from_disk(self, key: Any) -> Union[DataEntry, None]:
        if not isinstance(key, self.key_type):
            raise ValueError(f"Key must be of type {self.key_type}")

        serialized_key = json.dumps(key)

        partition = hash(key) % self.n_partitions
        file_name = f"{self.file_prefix}_{partition}.json"
        temp_file_name = f"{self.file_prefix}_temp.json"
        entry = None
        with open(file_name, "r") as original_file, open(temp_file_name, "w") as temp:
            for line in original_file:
                separator_index = line.find(self.KEY_VALUE_SEPARATOR)
                current_key = line[:separator_index]
                # If the key is found, we dont write it to the temp file
                # and we load the entry
                if current_key == serialized_key:
                    serialized_value = line[
                        separator_index + len(self.KEY_VALUE_SEPARATOR) :
                    ]
                    entry = self.DataEntry(
                        self.key_type,
                        self.value_type,
                        key,
                        json.loads(serialized_value),
                    )
                # Otherwise, we write the line to the temp file
                else:
                    temp.write(line)

        os.replace(temp_file_name, file_name)
        return entry

    def _cache_has_vacants(self) -> bool:
        return self.cached_entries < self.cache_vacants

    def _write_to_cache(self, key: Any, value: Any):
        if not isinstance(key, self.key_type):
            raise ValueError(f"Key must be of type {self.key_type}")

        if not isinstance(value, self.value_type):
            raise ValueError(f"Value must be of type {self.value_type}")

        if self.cached_entries >= self.cache_vacants:
            self._write_oldest_to_disk()

        self.cache[key] = value
        self.cached_entries += 1

    def add(self, key: Any, value: Any):
        if not isinstance(key, self.key_type):
            raise ValueError(f"Key must be of type {self.key_type}")

        if not isinstance(value, self.value_type):
            raise ValueError(f"Value must be of type {self.value_type}")

        stored_entry = self.get(key)

        # If the key is not in the cache or no merge function is provided,
        # we just write the new value to the cache
        if stored_entry is None or self.merge_function is None:
            if stored_entry is not None:
                logging.info(f"Overwriting entry for key {key}")

            if not self._cache_has_vacants():
                self._write_oldest_to_disk()
            self._write_to_cache(key, value)
            return

        # If the key is in the cache, we merge the new value with the stored one
        result = self.merge_function(stored_entry.value, value)
        stored_entry.value = result

        if not self._cache_has_vacants():
            self._write_oldest_to_disk()
        self._write_to_cache(key, result)

    def get(self, key: Any) -> Union[DataEntry, None]:
        if not isinstance(key, self.key_type):
            raise ValueError(f"Key must be of type {self.key_type}")

        # If the key is in the cache, we return a copy of the entry
        if key in self.cache:
            value = self.cache.get(key)
            return self.DataEntry(self.key_type, self.value_type, key, value)

        # If the key is not in the cache, and there are no entries in the files,
        # we return None
        if self.entries_in_files <= 0:
            return None

        # If the key is not in the cache, and there are entries in the files,
        # we pop the entry from the disk
        entry = self._pop_from_disk(key)

        # If the entry is not found in the disk, we return None
        if entry is None:
            return None

        # If the entry is found in the disk, we add it to the cache and return it
        if not self._cache_has_vacants():
            self._write_oldest_to_disk()

        self._write_to_cache(entry.key, entry.value)
        return entry

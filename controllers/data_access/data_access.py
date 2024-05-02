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
        # logging.info(f"Number before popping from cache: {self.cached_entries}")
        if self.cached_entries <= 0:
            raise ValueError("Cache is empty")

        try:
            first_cache_key = list(self.cache.keys())[0]
        except IndexError:
            raise IndexError(
                f"Cache is empty and should have {self.cached_entries} entries"
            )
        first_cache_value = self.cache.pop(first_cache_key)
        self.cached_entries -= 1

        # logging.info(f"Number after popping from cache: {self.cached_entries}")

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
        # logging.info(f"Number before committing: {self.cached_entries}")
        oldest_entry = self._pop_oldest_cache_entry()
        partition = self._partition_for_entry(oldest_entry)
        file_name = f"{self.file_prefix}_{partition}.json"

        serialized_key = json.dumps(oldest_entry.key)
        serialized_value = json.dumps(oldest_entry.value)

        entry = f"{serialized_key}{self.KEY_VALUE_SEPARATOR}{serialized_value}\n"
        logging.debug(f"Writing entry to disk: {entry}")
        with open(file_name, "a") as f:
            f.write(entry)

        self.entries_in_files += 1

        # logging.info(f"Number after committing: {self.cached_entries}")

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
                    logging.debug(f"Found entry in disk for: {key}")
                    entry = self.DataEntry(
                        self.key_type,
                        self.value_type,
                        key,
                        json.loads(serialized_value),
                    )
                    self.entries_in_files -= 1
                # Otherwise, we write the line to the temp file
                else:
                    temp.write(line)

        os.replace(temp_file_name, file_name)
        return entry

    def _cache_has_vacants(self) -> bool:
        return self.cached_entries < self.cache_vacants

    def clear(self):
        logging.info("Clearing cache and files")
        self.cache.clear()
        self.cached_entries = 0
        self.entries_in_files = 0
        for i in range(self.n_partitions):
            file_name = f"{self.file_prefix}_{i}.json"
            if os.path.exists(file_name):
                os.remove(file_name)

    def _write_to_cache(self, key: Any, value: Any):
        # logging.info(f"Number before adding to cache: {self.cached_entries}")
        if not isinstance(key, self.key_type):
            raise ValueError(f"Key must be of type {self.key_type}")

        if not isinstance(value, self.value_type):
            raise ValueError(f"Value must be of type {self.value_type}")

        if not self._cache_has_vacants():
            self._write_oldest_to_disk()

        self.cache[key] = value
        self.cached_entries += 1
        # logging.info(f"Number after adding to cache: {self.cached_entries}")

    def add(self, key: Any, value: Any):
        # logging.info(f"Number before adding: {self.cached_entries}")
        if not isinstance(key, self.key_type):
            raise ValueError(f"Key must be of type {self.key_type}")

        if not isinstance(value, self.value_type):
            raise ValueError(f"Value must be of type {self.value_type}")

        if key in self.cache:
            cached_value = self.cache.pop(key)
            stored_entry = self.DataEntry(
                self.key_type, self.value_type, key, cached_value
            )
            self.cached_entries -= 1
        elif self.entries_in_files > 0:
            stored_entry = self._pop_from_disk(key)
        else:
            stored_entry = None

        # If the key is not in the cache or no merge function is provided,
        # we just write the new value to the cache
        if stored_entry is None or self.merge_function is None:
            if stored_entry is not None:
                logging.debug(f"Overwriting entry for key {key}")
            else:
                logging.debug(f"Adding new entry for key {key}")

            if not self._cache_has_vacants():
                self._write_oldest_to_disk()
            self._write_to_cache(key, value)

            # logging.info(f"Number after adding: {self.cached_entries}")
            return

        # If the key is in the cache, we merge the new value with the stored one
        result = self.merge_function(stored_entry.value, value)
        logging.debug(
            f"Merge Function Called for {key}: {stored_entry.value} + {value} = {result}"
        )

        if not self._cache_has_vacants():
            self._write_oldest_to_disk()
        self._write_to_cache(key, result)

    def get(self, key: Any) -> Union[DataEntry, None]:
        # logging.info(f"Number before getting: {self.cached_entries}")
        if not isinstance(key, self.key_type):
            raise ValueError(f"Key must be of type {self.key_type}")

        # If the key is in the cache, we return a copy of the entry
        if key in self.cache:
            value = self.cache.get(key)
            # logging.info(f"Number after getting: {self.cached_entries}")
            return self.DataEntry(self.key_type, self.value_type, key, value)

        # If the key is not in the cache, and there are no entries in the files,
        # we return None
        if self.entries_in_files <= 0:
            # logging.info(f"Number after getting: {self.cached_entries}")
            return None

        # If the key is not in the cache, and there are entries in the files,
        # we pop the entry from the disk
        entry = self._pop_from_disk(key)

        # If the entry is not found in the disk, we return None
        if entry is None:
            # logging.info(f"Number after getting: {self.cached_entries}")
            return None

        # If the entry is found in the disk, we add it to the cache and return it
        if not self._cache_has_vacants():
            self._write_oldest_to_disk()

        self._write_to_cache(entry.key, entry.value)
        # logging.info(f"Number after getting: {self.cached_entries}")
        return entry

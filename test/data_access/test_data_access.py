import os
from src.data_access.data_access import DataAccess
import unittest
import pytest

NUM_OF_PARTITIONS_TO_TEST = [1, 2, 3]
FILE_PREFIX = "test_data_access"

class TestDataAccess():
    @classmethod
    def teardown_class(cls) -> None:
        # Remove all files created during the tests
        for i in range(max(NUM_OF_PARTITIONS_TO_TEST)):
            file_name = f"{FILE_PREFIX}_{i}.json"
            if os.path.exists(file_name):
                os.remove(file_name)

    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_n_elements(self, n_partitions):
        data_access = DataAccess(
            key_type=str,
            value_type=str,
            cache_vacants=1,
            n_partitions=n_partitions,
            file_prefix=FILE_PREFIX,
        )
        assert data_access.n_elements_in_cache() == 0
        assert data_access.n_elements_in_files() == 0

        data_access.add("Key", "Value")
        assert data_access.n_elements_in_cache() == 1
        assert data_access.n_elements_in_files() == 0

        data_access.add("Key2", "Value2")
        assert data_access.n_elements_in_cache() == 1
        assert data_access.n_elements_in_files() == 1

        data_access.add("Key3", "Value3")
        assert data_access.n_elements_in_cache() == 1
        assert data_access.n_elements_in_files() == 2

        data_access.add("Key4", "Value4")
        assert data_access.n_elements_in_cache() == 1
        assert data_access.n_elements_in_files() == 3

        data_access.clear()
        assert data_access.n_elements_in_cache() == 0
        assert data_access.n_elements_in_files() == 0
    
    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_get_from_cache(self, n_partitions):
        data_access = DataAccess(
            key_type=str,
            value_type=str,
            cache_vacants=1,
            n_partitions=1,
            file_prefix=FILE_PREFIX,
        )
        data_access.add("Key1", "Value1")
        result = data_access.get("Key1")
        assert result is not None and result.value == "Value1"

        data_access.add("Key2", "Value2")
        result = data_access.get("Key2")
        assert result is not None and result.value == "Value2"

    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_get_from_files(self, n_partitions):
        data_access = DataAccess(
            key_type=str,
            value_type=str,
            cache_vacants=1,
            n_partitions=1,
            file_prefix=FILE_PREFIX,
        )
        data_access.add("Key1", "Value1")
        data_access.add("Key2", "Value2")
        result = data_access.get("Key1")
        assert result is not None and result.value == "Value1"

    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_non_existing_key(self, n_partitions):
        data_access = DataAccess(
            key_type=str,
            value_type=str,
            cache_vacants=1,
            n_partitions=1,
            file_prefix=FILE_PREFIX,
        )
        assert data_access.get("Key2") is None
        data_access.add("Key1", "Value1")
        assert data_access.get("Key2") is None

    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_lists_as_types_from_cache(self, n_partitions):
        data_access = DataAccess(
            key_type=tuple,
            value_type=tuple,
            cache_vacants=1,
            n_partitions=1,
            file_prefix=FILE_PREFIX,
        )

        key = (1, 2, 3)
        value = (2, 4, 6)
        data_access.add(key, value)
        result = data_access.get(key)
        assert result is not None and result.value == value

    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_tuples_as_types_from_files(self, n_partitions):
        data_access = DataAccess(
            key_type=tuple,
            value_type=tuple,
            cache_vacants=1,
            n_partitions=1,
            file_prefix=FILE_PREFIX,
        )

        key1 = (1, 2, 3)
        value1 = (2, 4, 6)

        key2 = (4, 5, 6)
        value2 = (8, 10, 12)

        data_access.add(key1, value1)
        data_access.add(key2, value2)

        result = data_access.get(key1)
        assert result is not None and result.value == value1

    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_merge_function_cache(self, n_partitions):
        data_access = DataAccess(
            key_type=str,
            value_type=int,
            cache_vacants=1,
            n_partitions=1,
            file_prefix=FILE_PREFIX,
            merge_function=lambda x, y: x + y,
        )

        data_access.add("Key", 1)
        data_access.add("Key", 2)
        result = data_access.get("Key")
        assert result is not None and result.value == 3

    @pytest.mark.parametrize("n_partitions", NUM_OF_PARTITIONS_TO_TEST)
    def test_merge_function_files(self, n_partitions):
        data_access = DataAccess(
            key_type=str,
            value_type=int,
            cache_vacants=1,
            n_partitions=1,
            file_prefix=FILE_PREFIX,
            merge_function=lambda x, y: x + y,
        )

        data_access.add("Key1", 1)
        data_access.add("Key2", 10)
        data_access.add("Key1", 4)
        result = data_access.get("Key1")
        assert result is not None and result.value == 5
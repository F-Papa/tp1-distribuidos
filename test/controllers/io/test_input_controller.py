from typing import Callable

import pytest
from src.controllers.io.input.input_controller import (
    InputController,
    CATEGORY_FILTER_QUEUE,
    DATE_FILTER_QUEUE,
    DECADES_COUNTER_QUEUE,
)
from src.messaging.goutong import Message

BATCH_SIZES = [1, 3, 100, 500]

class MockMessaging:
    def __init__(self):
        self.dict = {}

    def listen(self):
        pass

    def add_queues(self, *args):
        pass

    def set_callback(self, queue_name: str, callback: Callable, args: tuple = ()):
        pass

    def send_to_queue(self, queue_name: str, message: Message):
        if self.dict.get(queue_name) is None:
            self.dict[queue_name] = []

        # Append the message to the queue
        self.dict[queue_name].append(message)

@pytest.mark.parametrize("items_per_batch", BATCH_SIZES)
def test_data_is_dispatched_correctly(items_per_batch: int):
    # Setup
    config = {
        "LOGGING_LEVEL": "INFO",
        "ITEMS_PER_BATCH": items_per_batch,
        "BACKLOG": 1,
        "SERVER_PORT": 1234,
        "MESSAGING_HOST": "test",
    }

    messaging = MockMessaging()
    input_controller = InputController(config, messaging)  # type: ignore

    # Data
    data_in = [
        {
            "conn_id": 1,
            "data": [
                {
                    "title": "Book1",
                    "year": 1990,
                    "authors": ["Author1A", "Author1B"],
                    "publisher": "Publisher1",
                    "categories": ["Category1A", "Category1B"],
                },
                {
                    "title": "Book2",
                    "year": 1991,
                    "authors": ["Author2A", "Author2B"],
                    "publisher": "Publisher2",
                    "categories": ["Category2A", "Category2B"],
                },
            ],
            "EOF": False,
        },
        {
            "conn_id": 1,
            "data": [
                {
                    "title": "Book3",
                    "year": 1992,
                    "authors": ["Author3A", "Author3B"],
                    "publisher": "Publisher3",
                    "categories": ["Category3A", "Category3B"],
                },
                {
                    "title": "Book4",
                    "year": 1993,
                    "authors": ["Author4A", "Author4B"],
                    "publisher": "Publisher4",
                    "categories": ["Category4A", "Category4B"],
                },
            ],
            "EOF": True,
        },
    ]

    # Expected
    expected_date_queue_data = [
        {
            "title": "Book1",
            "year": 1990,
            "authors": ["Author1A", "Author1B"],
            "publisher": "Publisher1",
            "categories": ["Category1A", "Category1B"]
        },
        {
            "title": "Book2",
            "year": 1991,
            "authors": ["Author2A", "Author2B"],
            "publisher": "Publisher2",
            "categories": ["Category2A", "Category2B"]
        },
        {
            "title": "Book3",
            "year": 1992,
            "authors": ["Author3A", "Author3B"],
            "publisher": "Publisher3",
            "categories": ["Category3A", "Category3B"]
        },
        {
            "title": "Book4",
            "year": 1993,
            "authors": ["Author4A", "Author4B"],
            "publisher": "Publisher4",
            "categories": ["Category4A", "Category4B"]
        },
    ]

    expected_decade_counter_queue_data = [
        {"decade": 1990, "authors": ["Author1A", "Author1B"]},
        {"decade": 1990, "authors": ["Author2A", "Author2B"]},
        {"decade": 1990, "authors": ["Author3A", "Author3B"]},
        {"decade": 1990, "authors": ["Author4A", "Author4B"]},
    ]

    expected_category_queue_data = [
        {"title": "Book1", "categories": ["Category1A", "Category1B"]},
        {"title": "Book2", "categories": ["Category2A", "Category2B"]},
        {"title": "Book3", "categories": ["Category3A", "Category3B"]},
        {"title": "Book4", "categories": ["Category4A", "Category4B"]},
    ]

    # Test
    for d in data_in:
        msg = Message(d)
        input_controller._dispatch_books(messaging, msg)  # type: ignore

    # Get the data from the queues
    date_queue_msgs: list[Message] = messaging.dict.get(DATE_FILTER_QUEUE)  # type: ignore
    date_queue_data = []
    print(date_queue_msgs)
    for msg in date_queue_msgs:  # type: ignore
        date_queue_data.extend(msg.get("data"))

    decade_counter_queue_msgs = messaging.dict.get(DECADES_COUNTER_QUEUE)
    decade_counter_queue_data = []
    for msg in decade_counter_queue_msgs:  # type: ignore
        decade_counter_queue_data.extend(msg.get("data"))

    category_queue_msgs = messaging.dict.get(CATEGORY_FILTER_QUEUE)
    category_queue_data = []
    for msg in category_queue_msgs:  # type: ignore
        category_queue_data.extend(msg.get("data"))

    # Verify
    assert date_queue_data == expected_date_queue_data
    assert decade_counter_queue_data == expected_decade_counter_queue_data
    assert category_queue_data == expected_category_queue_data

@pytest.mark.parametrize("items_per_batch", BATCH_SIZES)
def test_end_of_books_not_receiving_from_connection_after_EOF(items_per_batch: int):
    config = {
        "LOGGING_LEVEL": "INFO",
        "ITEMS_PER_BATCH": items_per_batch,
        "BACKLOG": 1,
        "SERVER_PORT": 1234,
        "MESSAGING_HOST": "test",
    }

    messaging = MockMessaging()
    input_controller = InputController(config, messaging)  # type: ignore

        # Data
    data_in = [
        {
            "conn_id": 1,
            "data": [
                {
                    "title": "Book1",
                    "year": 1990,
                    "authors": ["Author1A", "Author1B"],
                    "publisher": "Publisher1",
                    "categories": ["Category1A", "Category1B"],
                },
                {
                    "title": "Book2",
                    "year": 1991,
                    "authors": ["Author2A", "Author2B"],
                    "publisher": "Publisher2",
                    "categories": ["Category2A", "Category2B"],
                },
            ],
            "EOF": False,
        },
        {
            "conn_id": 2,
            "data": [
                {
                    "title": "Book3",
                    "year": 1992,
                    "authors": ["Author3A", "Author3B"],
                    "publisher": "Publisher3",
                    "categories": ["Category3A", "Category3B"],
                },
                {
                    "title": "Book4",
                    "year": 1993,
                    "authors": ["Author4A", "Author4B"],
                    "publisher": "Publisher4",
                    "categories": ["Category4A", "Category4B"],
                },
            ],
            "EOF": True,
        },
    ]

    for d in data_in:
        msg = Message(d)
        input_controller._dispatch_books(messaging, msg)  # type: ignore

    connection_ids = input_controller.receiving_from_connections()
    assert 1 in connection_ids
    assert 2 not in connection_ids
import csv
from typing import Callable
from src.messaging.message import Message
from src.boundary.boundary import *
import src.client.common.parsing as parsing

import pytest

BOOK_TEST_FILES = [
    "data/test/books_data11.csv",
    "data/test/books_data500.csv",
]

REVIEW_TEST_FILES = [
    "data/test/ratings_200.csv",
    "data/test/ratings_1K.csv",
]

SOCKET_READ_WRITE_LIMITS = [1024, 500]

BATCH_SIZES = [1, 3, 100, 500]


class MockSocket:
    def __init__(self, read_write_limit: int = 104):
        self.data = b""
        self.dict = {}
        self.read_write_limit = read_write_limit
        # with open("data.json", "w") as f:
        #     pass

    def recv(self, size: int):
        to_return = self.data[:size]
        self.data = self.data[size:]
        return to_return

    def close(self):
        pass

    def send(self, data: bytes):
        self.data += data[: self.read_write_limit]
        return min(len(data), self.read_write_limit)

    def encode_and_send(self, data: dict):
        encoded_json = json.dumps(data).encode("utf-8")

        sent_bytes = 0
        to_send = (
            len(encoded_json).to_bytes(BATCH_SIZE_LEN, byteorder="big") + encoded_json
        )
        while sent_bytes < len(to_send):
            sent_bytes += self.send(to_send[sent_bytes:])

        # with open("data.json", "a") as f:
        #     f.write(json.dumps(data) + "\n")


class MockMessaging:
    def __init__(self, *args, **kwargs):
        self.dict = {}

    def add_queues(self, *args):
        pass

    def listen(self):
        pass

    def send_to_queue(self, queue_name: str, message: Message):
        if self.dict.get(queue_name) is None:
            self.dict[queue_name] = []

        # Append the message to the queue
        self.dict[queue_name].append(message.marshal())

    def set_callback(self, queue_name: str, callback: Callable, auto_ack: bool = True, args: tuple = ()):
        pass


@pytest.mark.parametrize("book_file", BOOK_TEST_FILES)
@pytest.mark.parametrize("items_per_batch", BATCH_SIZES)
@pytest.mark.parametrize("reviews_file", REVIEW_TEST_FILES)
@pytest.mark.parametrize("read_write_limit", SOCKET_READ_WRITE_LIMITS)
def test_books_and_reviews_are_dispatched_to_the_queues_correctly(
    book_file: str, reviews_file: str, items_per_batch: int, read_write_limit: int
):
    # Setup
    config = {
        "LOGGING_LEVEL": "INFO",
        "ITEMS_PER_BATCH": items_per_batch,
        "BACKLOG": 1,
        "SERVER_PORT": 1234,
        "MESSAGING_HOST": "test",
        "MESSAGING_PORT": 5672,
    }

    # Save the data in a buffer instead of sending it through TCP
    mock_socket = MockSocket(read_write_limit)

    books_sent = []
    reviews_sent = []

    # Send Books
    with open(book_file, "r") as f:
        batch = []
        reader = csv.DictReader(f)
        for row in reader:
            parsed_row = parsing.parse_book_line(row)
            books_sent.append(parsed_row)
            batch.append(parsed_row)
            if len(batch) == config["ITEMS_PER_BATCH"]:
                mock_socket.encode_and_send({"data": batch})
                batch.clear()

        mock_socket.encode_and_send({"data": batch, "EOF": True})
        batch.clear()

    # Send Reviews
    with open(reviews_file, "r") as f:
        batch = []
        reader = csv.DictReader(f)
        for row in reader:
            parsed_row = parsing.parse_review_line(row)
            reviews_sent.append(parsed_row)
            batch.append(parsed_row)
            if len(batch) == config["ITEMS_PER_BATCH"]:
                mock_socket.encode_and_send({"data": batch})
                batch.clear()

        mock_socket.encode_and_send({"data": batch, "EOF": True})
        batch.clear()

    connection = ClientConnection(
        mock_socket,  # type: ignore
        1,
        items_per_batch,
        MockMessaging,
        config["MESSAGING_HOST"],
        config["MESSAGING_PORT"],
    )

    # Test
    connection.handle_connection()  # type: ignore

    # Compare the sent and received titles
    book_messages_received = connection.messaging.dict[BOOKS_QUEUE]  # type: ignore
    books_received = []
    for item in book_messages_received:
        books_in_msg = Message.unmarshal(item).get("data")
        books_received.extend(books_in_msg)

    reviews_messages_received = connection.messaging.dict[REVIEWS_QUEUE]  # type: ignore
    reviews_received = []
    for item in reviews_messages_received:
        reviews_in_msg = Message.unmarshal(item).get("data")
        reviews_received.extend(reviews_in_msg)

    assert books_sent == books_received
    assert reviews_sent == reviews_received


@pytest.mark.parametrize("items_per_batch", BATCH_SIZES)
@pytest.mark.parametrize("read_write_limit", SOCKET_READ_WRITE_LIMITS)
def test_results_are_sent_back_correctly(items_per_batch: int, read_write_limit: int):
    config = {
        "LOGGING_LEVEL": "INFO",
        "ITEMS_PER_BATCH": items_per_batch,
        "BACKLOG": 1,
        "SERVER_PORT": 1234,
        "MESSAGING_HOST": "test",
        "MESSAGING_PORT": 5672,
    }

    results = [
        {"queries": [4], "data": [{"title": "title1"}, {"title": "title2"}]},
        {"queries": [3], "data": [{"title": "title2", "authors": ["author2"]}]},
        {
            "queries": [2],
            "data": [{"author": "author3", "author": "author4", "author": "author5"}],
        },
        {"queries": [1], "data": [{"title": "title4"}]},
        {"queries": [5], "data": [{"title": "title5"}]},
    ]

    mock_socket = MockSocket(read_write_limit)
    connection = ClientConnection(
        mock_socket,  # type: ignore
        1,
        items_per_batch,
        MockMessaging,
        config["MESSAGING_HOST"],
        config["MESSAGING_PORT"],
    )

    for r in results:
        msg = Message(r)
        connection.forward_results(MockMessaging, msg)  # type: ignore

    expected = b""
    for r in results:
        encoded = json.dumps(r).encode("utf-8")
        expected += len(encoded).to_bytes(BATCH_SIZE_LEN, byteorder="big") + encoded

    assert mock_socket.data == expected

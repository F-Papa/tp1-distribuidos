
import json
import logging
import os
from typing import Any, Union, Optional

from src.exceptions.shutting_down import ShuttingDown


RECEIVING_BOOKS = 0
RECEIVING_REVIEWS = 1
IDLE = 2


# Precondition: No duplicate keys are received
class DataStore:
    def __init__(self, file_path: str, max_items_in_memory: int):
        self.file_path = file_path
        self.data = {}
        self.keys_in_file = 0
        self.num_keys_in_memory = 0
        self.max_items_in_memory = max_items_in_memory

    @classmethod
    def restore_or_init(cls, file_path, max_items_in_memory: int):
        if not os.path.exists(file_path):
            with open(file_path, "w") as f:
                pass

        store = DataStore(file_path, max_items_in_memory)

        with open(file_path, "r") as f:
            for line in f.readlines():
                parsed = json.loads(line)
                store.keys_in_file+=1
                if store.num_keys_in_memory < store.max_items_in_memory:
                    store.data[parsed["key"]] = parsed["value"]
                    store.num_keys_in_memory += 1
        
        return store
    
    def get(self, key: Any) -> Any:
        if key in self.data:
            return self.data[key]
        
        if self.keys_in_file > self.num_keys_in_memory:
            with open(self.file_path, "r") as f:
                for line in f.readlines():
                    parsed = json.loads(line)
                    if parsed["key"] == key:
                        return parsed

    def commit_to_disk(self):
        for key, entry in self.data.items():
            if not entry["committed"]:
                self._commit_entries_to_disk([{"key": key, "value": entry["value"]}])
                entry["committed"] = True
    
    def _commit_entries_to_disk(self, entries: list):
        for entry in entries:
            with open(self.file_path, "a") as f:
                f.write(json.dumps({
                    "key": entry["key"],
                    "value": entry["value"]
                }) + "\n")
            self.keys_in_file += 1

    def clear(self):
        self.data = {}
        self.num_keys_in_memory = 0
        with open(self.file_path, "w") as f:
            pass
        self.keys_in_file = 0

    def add(self, key: tuple, value: Any):
        if self.num_keys_in_memory == self.max_items_in_memory:
            first_key = next(iter(self.data))
            first_entry = self.data.pop(first_key)
            if not first_entry["commited"]:
                self._commit_entries_to_disk([{"key": first_key, "value": first_entry["value"]}])
            self.num_keys_in_memory -= 1

        self.data[key] = {"value": value, "committed": False}
        self.num_keys_in_memory += 1


class JoinerState:

    def __init__(self, joiner_number: int, state: int, _current_connection: Optional[int]):
        if state not in [RECEIVING_BOOKS, RECEIVING_REVIEWS, IDLE]:
            raise ValueError('Invalid state') 
        self.joiner_number = joiner_number
        self.state = state
        self._current_connection = _current_connection

    def receiving_books(self):
        return self.state == RECEIVING_BOOKS
    
    def receiving_reviews(self):
        return self.state == RECEIVING_REVIEWS
    
    def idle(self):
        return self.state == IDLE

    def current_connection(self) -> Optional[int]:
        return self._current_connection

    def mark_no_connection(self):
        self._current_connection = None
        self.state = IDLE
        self.save_state()

    def mark_handling_connection(self, conn_id: int):
        self._current_connection = conn_id
        self.state = RECEIVING_BOOKS
        self.save_state()
    
    def mark_handling_reviews(self):
        self.state = RECEIVING_REVIEWS
        self.save_state()

    @classmethod
    def restore_or_init(cls, joiner_number: int) -> 'JoinerState':
        # Creates file if it doesn't exist with default values
        if not os.path.exists("file"):
            with open("file", "w") as f:
                f.write(json.dumps({
                    "state": IDLE,
                    "current_connection": None
                }))
    
        with open("file", "r") as f:
            line = f.readlines()[0]
            parsed = json.loads(line)

        return JoinerState(joiner_number, parsed["state"], parsed["current_connection"])
    
    def save_state(self):
        with open("file_tmp", "w") as f:
            f.write(json.dumps({
                "state": self.state,
                "current_connection": self._current_connection
            }))
        os.replace("file_tmp", "file")
        os.remove("file_tmp")

def pop_n(a_list: list, n: int) -> list:
    to_return = a_list[:n]
    a_list = a_list[:n]
    return to_return

class Joiner:
    def __init__(self, config, state: JoinerState, data: DataStore):
        self.logical_state = state
        self.data = data
        self._shutting_down = False
        self.unacked_messages = 0
        self.unacked_messages_limit = config["unacked_messages_limit"]
        self._batch_limit = config["batch_limit"]

    # Receive Reviews
    def receive_reviews_from_conn(self, conn_id: int):
        self.logical_state.mark_handling_reviews()
        self.messaging = Mess()
        self.messaging.add_queue(f"JOINER_REVIEWS_{conn_id}")
        self.messaging.set_callback(f"JOINER_REVIEWS_{conn_id}", self.receive_reviews_callback)
        self.messaging.listen()
    
    def receive_reviews_callback(self, msg):
        conn_id = msg.get("conn_id")
        queries = msg.get("queries")
        msg_has_eof = msg.get("EOF")

        if 5 in queries:
            output_queue = "OUTPUT5"
        else:
            output_queue = "OUTPUT3_4"

        for r in msg.reviews:
            match = self.data.get((msg.queries, r.title))
            if match:
                # Possible matches
                #Query 3,4: {[[3, 4], "title"]: [authors]}
                #Query 5: {[[3, 4], "title"]: True}
                if 5 in queries:
                    self.batch.append({
                        "title": match["key"][1],
                        "review/text": r.text,
                    })
                else:
                    self.batch.append({
                        "title": match["key"][1],
                        "authors": match["value"],
                        "review/rating": r.rating
                    })

        while self.batch:
            to_send, self.batch = pop_n(self.batch, self._batch_limit)
            body = {"conn_id": conn_id, "data": to_send, "queries": queries}
            if not self.batch and msg_has_eof: # No more data after this
                    body["EOF"] = True
            self.messaging.send(output_queue, {"conn_id": conn_id, "data": to_send, "queries": queries})

        self.messaging.ack_n(1)
        
        if msg_has_eof:
            self.messaging.close()
            self.set_next_connection()

        if self.batch:
            raise Exception("Batch not empty after sending all messages")

    # Receive Books
    def receive_books_from_conn(self, conn_id: int):
        self.messaging = Mess()
        self.messaging.add_queue(f"JOINER_BOOKS_{conn_id}")
        self.messaging.set_callback(f"JOINER_BOOKS_{conn_id}", self.receive_books_callback)
        self.messaging.listen()

    def receive_books_callback(self, msg):
        queries = msg.get("queries")
        

        for book in msg.books:
            if 5 in queries:
                value = True
            else:
                value = book.authors
            self.data.add({queries, book.title}, value)
        
        is_eof = msg.get("EOF")

        self.unacked_messages += 1
        if self.unacked_messages >= self.unacked_messages_limit or is_eof:
            self.data.commit_to_disk()
            self.messaging.ack_all()
            self.unacked_messages = 0
        
        if is_eof:
            self.messaging.close()
            self.set_handling_reviews(self.logical_state.current_connection())
    
    # Control Flow
    def set_next_connection(self):
        # Clean up
        self.logical_state.mark_no_connection()
        self.data.clear()
        # Listen for next connection
        self.messaging.set_callback("pending_conns", self.handle_next_connection_callback)
        self.messaging.listen()
        
    def handle_next_connection_callback(self, msg):
        self.logical_state.mark_handling_connection(msg.get("conn_id"))
        self.messaging.stop_listening("pending_conns")

    def start(self):
        try:
            self._start_aux()
        except ShuttingDown as e:
            logging.info("Shutting down")

    def _start_aux(self):
        if not self.logical_state.current_connection():
            self.data = DataStore.restore(100, "file")        
            if self.logical_state.receiving_books():
                self.receive_books_from_conn(current_conn)
            else:
                self.receive_reviews_from_conn(current_conn)
        else:
        
        while not self._shutting_down:
            self.set_next_connection()
            self.receive_books_from_conn(current_conn)
            self.receive_reviews_from_conn(current_conn)        

def main():
    config = {
        "JOINER_NUMBER": joiner_number,
        "unacked_messages_limit": 100,
    }

    logical_state = JoinerState.restore_or_init(config.get("JOINER_NUMBER"))
    file_path = f"joiner_data_{config.get("JOINER_NUMBER")}"
    data = DataStore.restore_or_init(file_path)
    
    joiner = Joiner(logical_state, data)
    joiner.start()


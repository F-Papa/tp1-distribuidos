from collections import defaultdict
import json
import logging
import os

class ControllerState:
    READY_MARKER = "=-#READY#-="

    def __init__(
        self,
        controller_id: str,
        file_path: str,
        temp_file_path: str,
        extra_fields: dict,
    ):
        self.committed = True # Deprecated
        self.controller_id = controller_id
        self.next_transaction = 1 # Deprecated
        self.transactions_received = [] # Deprecated
        self.file_path = file_path
        self.temp_file_path = temp_file_path
        self.extra_fields = set(extra_fields.keys())

        # New
        self.next_inbound_transactions_ids = defaultdict(lambda: 1)
        self.next_outbound_transaction_ids = defaultdict(lambda: 1)

        for key in extra_fields:
            setattr(self, key, extra_fields[key])
    
    def outbound_transaction_committed(self, queue: str):
        self.next_outbound_transaction_ids[queue] += 1

    def inbound_transaction_committed(self, sender: str):
        self.next_inbound_transactions_ids[sender] += 1
    
    def next_outbound_transaction_id(self, queue: str) -> int:
        return self.next_outbound_transaction_ids[queue]
    
    def next_inbound_transaction_id(self, sender: tuple) -> int:
        return self.next_inbound_transactions_ids[sender]

    def save_to_disk(self):
        pass

        to_save = {
            "next_transaction": self.next_transaction,
            "transactions_received": self.transactions_received,
            "committed": self.committed,
        }

        for key in self.extra_fields:
            to_save[key] = getattr(self, key)

        with open(self.temp_file_path, "w") as f:
            f.write(json.dumps(to_save) + "\n")
            f.write(self.READY_MARKER)

        os.replace(self.temp_file_path, self.file_path)

    def get(self, key: str):
        return getattr(self, key)

    def set(self, key: str, value):
        if not key in self.__dict__:
            raise Exception(f"{key} was not declared in the constructor")
        setattr(self, key, value)

    def update_from_file(self):
        with open(self.file_path, "r") as f:
            file_lines = f.readlines()

        if not self._is_file_valid(file_lines):
            raise Exception("Invalid State File")
        else:
            logging.info("Loading state from file")

        state_in_file = json.loads(file_lines[0])

        for key in state_in_file:
            setattr(self, key, state_in_file[key])

    def _is_file_valid(self, lines: list):
        return len(lines) == 2 and lines[-1] == self.READY_MARKER

    def is_transaction_received(self, transaction_id: str) -> bool:
        return transaction_id in self.transactions_received


    # Deprecated
    def id_for_next_transaction(self) -> str:
        return f"{self.controller_id}#{self.next_transaction}"

    # Deprecated
    def mark_transaction_received(self, transaction_id: str):
        self.transactions_received.append(transaction_id)

    # Deprecated
    def mark_transaction_committed(self):
        self.committed = True
        self.next_transaction += 1
        self.save_to_disk()

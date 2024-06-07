from collections import defaultdict
import json
import logging
import os


class ControllerState:
    """Class to manage the state of a controller. The state is saved to disk and can be updated from disk. It also manages the transactions ids for the controller."""
    READY_MARKER = "=-#READY#-="

    def __init__(
        self,
        controller_id: str,
        file_path: str,
        temp_file_path: str,
        extra_fields: dict,
    ):
        self.committed = True  # Deprecated
        self.controller_id = controller_id
        self.next_transaction = 1  # Deprecated
        self.transactions_received = []  # Deprecated
        self.file_path = file_path
        self.temp_file_path = temp_file_path
        self.extra_fields = set(extra_fields.keys())

        # New
        self.next_inbound_transactions_ids = defaultdict(lambda: 1)
        self.next_outbound_transaction_ids = defaultdict(lambda: 1)

        for key in extra_fields:
            setattr(self, key, extra_fields[key])

    def outbound_transaction_committed(self, queue: str):
        """Increments the transaction id for the given queue, indicating that a new transaction was committed"""
        self.next_outbound_transaction_ids[queue] += 1

    def inbound_transaction_committed(self, sender: str):
        """Increments the transaction id for the given sender, indicating that one more transaction was received by that sender"""
        self.next_inbound_transactions_ids[sender] += 1

    def next_outbound_transaction_id(self, queue: str) -> int:
        """Returns the next transaction id for the given queue"""
        return self.next_outbound_transaction_ids[queue]

    def next_inbound_transaction_id(self, sender: tuple) -> int:
        """Returns the next expected transaction id from a given sender"""
        return self.next_inbound_transactions_ids[sender]

    def save_to_disk(self):
        """Saves the state to disk"""
        to_save = {
            "next_transaction": self.next_transaction,
            "transactions_received": self.transactions_received,
            "committed": self.committed,
            "next_inbound_transactions_ids": self.next_inbound_transactions_ids,
            "next_outbound_transaction_ids": self.next_outbound_transaction_ids,
        }

        for key in self.extra_fields:
            to_save[key] = getattr(self, key)

        with open(self.temp_file_path, "w") as f:
            f.write(json.dumps(to_save) + "\n")
            f.write(self.READY_MARKER)

        os.replace(self.temp_file_path, self.file_path)

    def get(self, key: str):
        """Returns the value of the given key"""
        return getattr(self, key)

    def set(self, key: str, value):
        """Sets the value of the given key"""
        if not key in self.__dict__:
            raise Exception(f"{key} was not declared in the constructor")
        setattr(self, key, value)

    def update_from_file(self):
        """Updates the state from the file. Throws an exception if the file is invalid"""
        with open(self.file_path, "r") as f:
            file_lines = f.readlines()

        if not self._is_file_valid(file_lines):
            raise Exception("Invalid State File")
        else:
            logging.info("Loading state from file")

        state_in_file = json.loads(file_lines[0])

        for key in state_in_file:
            # Default dict needs to be handled separately
            if key == "next_inbound_transactions_ids":
                for sender in state_in_file[key]:
                    self.next_inbound_transactions_ids[sender] = state_in_file[key][sender]
            # Default dict needs to be handled separately
            elif key == "next_outbound_transaction_ids":
                for queue in state_in_file[key]:
                    self.next_outbound_transaction_ids[queue] = state_in_file[key][queue]
            else:
                setattr(self, key, state_in_file[key])

    def _is_file_valid(self, lines: list):
        return len(lines) == 2 and lines[-1] == self.READY_MARKER

    def id_for_next_transaction(self) -> str:
        """Deprecated, don't use"""
        return f"{self.controller_id}#{self.next_transaction}"

    def mark_transaction_received(self, transaction_id: str):
        """Deprecated, don't use"""
        self.transactions_received.append(transaction_id)

    def mark_transaction_committed(self):
        """Deprecated, don't use"""
        self.committed = True
        self.next_transaction += 1
        self.save_to_disk()

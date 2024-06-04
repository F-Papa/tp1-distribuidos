from collections import defaultdict
import threading
from typing import Callable, Optional
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.message import Message

class ProvokedError(Exception):
    pass

class MockMessaging:
    def __init__(self, host: str, port: int, queues_to_export: list[str], msgs_to_consume: int, crash_on_listening: Optional[int] = None, crash_on_send: Optional[int] = None):
        self.queued_msgs = {}
        self.unacked_msgs = {}
        self.callbacks = {}
        self.delivery_id = 1
        self.consumer_id = 1
        self.queues_to_export = queues_to_export
        self.add_queues(*queues_to_export)
        self.exported_msgs = {}
        self.export_condvars = defaultdict(threading.Condition)
        self.msgs_to_consume = msgs_to_consume
        self.crash_on_listening = crash_on_listening
        self.crash_on_send = crash_on_send
        self.msgs_consumed = 0
        self.msgs_sent = 0

    def _should_listen(self):
        if self.msgs_consumed >= self.msgs_to_consume:
            return False

        for k, v in self.callbacks.items():
            if len(v) > 0:
                return True
        return False

    # This method is used to get the messages from the exported queues, only used for testing
    def get_msgs_from_queue(self, queue_name: str):
        with self.export_condvars[queue_name]:
            if not self.exported_msgs.get(queue_name):
                self.export_condvars[queue_name].wait()
            return self.exported_msgs[queue_name].pop(0)

    def add_queues(self, *args):
        for queue in args:
            if self.queued_msgs.get(queue) is None:
                print(f"Adding queue {queue}")
                self.queued_msgs[queue] = []
                self.callbacks[queue] = []
                self.unacked_msgs[queue] = []

    def listen(self):
        if not self._should_listen():
            print("Max listenings reached.")
            raise ShuttingDown

        for queue, v in self.callbacks.items():
            if v:
                print(f"Listening to {queue}...")
        

        while self._should_listen():
            all_queues = list(self.callbacks.keys())
            print(f"Callbacks: {self.callbacks}")
            for queue in all_queues:
                if not self.queued_msgs[queue]:
                    continue
                if not self.callbacks.get(queue):
                    continue
                
                msg_str = self.queued_msgs[queue][0]
                
                if self.crash_on_listening:
                    if self.msgs_consumed == self.crash_on_listening:
                        self.msgs_consumed += 1
                        print(f"Crashing on listening: {self.crash_on_listening} | Msg: {msg_str}")
                        raise ProvokedError()
                
                self.queued_msgs[queue].pop(0)
                
                
                
                msg = Message.unmarshal(msg_str).with_id(self.delivery_id).from_queue(queue)
                


                callback, args, auto_ack = self.callbacks[queue][0]
                
                if not auto_ack:
                    self.unacked_msgs[self.delivery_id] = msg

                callback(self, msg, *args)
                self.msgs_consumed += 1
                self.delivery_id += 1
    
    def ack_delivery(self, delivery_id: int):
        if not self.unacked_msgs.get(delivery_id):
            raise ValueError(f"Delivery ID {delivery_id} not found in unacked messages.")
        del self.unacked_msgs[delivery_id]

    def send_to_queue(self, queue_name: str, message: Message):
        if self.queued_msgs.get(queue_name) is None:
            self.queued_msgs[queue_name] = []

        print(f"About to send: {message.marshal()}")        
        if self.crash_on_send:
            if self.msgs_sent == self.crash_on_send:
                self.msgs_sent += 1
                print(f"Crashing on send")
                raise ProvokedError()

        if queue_name in self.queues_to_export:

            if self.exported_msgs.get(queue_name) is None:
                self.exported_msgs[queue_name] = []
            self.exported_msgs[queue_name].append(message.marshal())
            with self.export_condvars[queue_name]:
                self.export_condvars[queue_name].notify()
        # Append the message to the queue
        else:
            self.queued_msgs[queue_name].append(message.marshal())
        
        self.msgs_sent += 1

    def set_callback(self, queue_name: str, callback: Callable, auto_ack: bool = True, args: tuple = ()):
        print(f"Callback set | Queue: {queue_name} | Function: {callback.__name__} | Auto Ack: {auto_ack} | Args: {args}")
        if self.callbacks.get(queue_name) is None:
            self.callbacks[queue_name] = []

        self.callbacks[queue_name].append((callback, args, auto_ack))

    def stop_consuming(self, queue_name: str):
        print(f"Stopped consuming {queue_name}")
        del self.callbacks[queue_name]

    def close(self):
        print("Closing connection")
        pass
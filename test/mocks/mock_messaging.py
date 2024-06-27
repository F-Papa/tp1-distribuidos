from collections import defaultdict
import threading
from typing import Callable, Optional
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.message import Message
import chalk

class ProvokedError(Exception):
    pass


class MockMessaging:
    def __init__(
        self,
        sender_id: str,
        host: str,
        port: int,
        times_to_listen: int,
        crash_on_listening: Optional[int] = None,
        crash_on_send: Optional[int] = None,
    ):
        self.sender_id = sender_id
        self.queued_msgs = {}
        self.unacked_msgs = {}
        self.callbacks = {}
        self.delivery_id = 1
        self.consumer_id = 1
        self.condvars = defaultdict(threading.Condition)
        self.msgs_to_consume = times_to_listen
        self.crash_on_listening = crash_on_listening
        self.crash_on_send = crash_on_send
        self.msgs_consumed = 0
        self.broadcast_groups = {}
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
        if not self.queued_msgs.get(queue_name):
            with self.condvars[queue_name]:
                self.condvars[queue_name].wait()
        return self.queued_msgs[queue_name].pop(0)

    def _add_queues(self, *args):
        for queue in args:
            if not queue in self.queued_msgs.keys():
                print(f"Adding queue {queue}")
                self.queued_msgs[queue] = []
                self.callbacks[queue] = []
                self.unacked_msgs[queue] = []

    def _requeue_msg(self, msg: Message):
        print(chalk.red(f"*******Re-queuing message********"))

        self.send_to_queue(
            queue_name=msg.queue_name,
            message=msg,
            sender_id=msg.get("sender"),
        )

    def listen(self):
        if not self._should_listen():
            print("Max listenings reached.")
            raise ShuttingDown

        for queue, v in self.callbacks.items():
            if v:
                print(f"Listening to {queue}... ({len(self.queued_msgs[queue])} messages)")

        while self._should_listen():
            all_queues = list(self.callbacks.keys())

            # print(f"Callbacks: {self.callbacks}")
            for queue in all_queues:
                if not self.queued_msgs[queue] or not self.callbacks.get(queue):
                    continue

                msg_str = self.queued_msgs[queue].pop(0)
                msg = (
                        Message.unmarshal(msg_str)
                        .with_id(self.delivery_id)
                        .from_queue(queue)
                    ) 
                
                # if self.crash_on_listening:
                #     if self.msgs_consumed == self.crash_on_listening:
                #         self.msgs_consumed += 1
                #         print(
                #             f"Crashing on listening: {self.crash_on_listening} | Msg: {msg_str}"
                #         )
                #         # self.send_to_queue(message=msg, queue_name=queue, sender_id=msg.get("sender") + str(" (re-queued)"))
                #         raise ProvokedError()

                callback, args, auto_ack = self.callbacks[queue][0]

                if not auto_ack:
                    self.unacked_msgs[self.delivery_id] = msg
                try:
                    callback(self, msg, *args)
                except ProvokedError:
                    raise ProvokedError
                
                finally:
                    if self.delivery_id in self.unacked_msgs:
                        del self.unacked_msgs[self.delivery_id]
                        self._requeue_msg(msg)
                    self.delivery_id += 1            
                    self.msgs_consumed += 1
                

    def ack_delivery(self, delivery_id: int):
        if not self.unacked_msgs.get(delivery_id):
            raise ValueError(
                f"Delivery ID {delivery_id} not found in unacked messages."
            )
        del self.unacked_msgs[delivery_id]

    def send_to_queue(self, queue_name: str, message: Message, sender_id: Optional[str] = None):
        self._add_queues(queue_name)

        if not sender_id:
            sender_id = self.sender_id

        message = message.with_sender(sender_id)

        print(chalk.yellow("==========================================="))
        print(chalk.yellow(f"FROM: {sender_id} | TO: {queue_name}"))
        print(chalk.yellow(message.marshal()))
        print(chalk.yellow("==========================================="))

        if self.crash_on_send:
            if self.msgs_sent + 1 == self.crash_on_send:
                self.msgs_sent += 1
                print(chalk.red(f"Crashing on send"))
                raise ProvokedError()

        if condvar := self.condvars.get(queue_name):
            with condvar:
                condvar.notify()
        self.queued_msgs[queue_name].append(message.marshal())
        self.msgs_sent += 1

    def set_callback(
        self,
        queue_name: str,
        callback: Callable,
        auto_ack: bool = True,
        args: tuple = (),
    ):
        # print(
        #     f"Callback set | Queue: {queue_name} | Function: {callback.__name__} | Auto Ack: {auto_ack} | Args: {args}"
        # )

        self._add_queues(queue_name)
        self.callbacks[queue_name].append((callback, args, auto_ack))

    def stop_consuming(self, queue_name: str):
        print(f"Stopped consuming {queue_name}")
        self.callbacks[queue_name] = []

    def add_broadcast_group(self, group_name: str, queue_names: list[str]):
        self._add_queues(*queue_names)
        self.broadcast_groups[group_name] = queue_names

    def broadcast_to_group(self, group_name: str, message: Message):
        for q in self.broadcast_groups[group_name]:
            self.send_to_queue(q, message)

    def close(self):
        print("Closing connection")
        pass

"""
Goutong is a middleware that abstracts MOM (Message Oriented Middleware) for the system.
It uses RabbitMQ.
"""

import logging
import pika
from typing import Callable
from messaging.message import Message


class Goutong:
    def __init__(self, host: str = "rabbit", port: int = 5672):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port)
        )
        self.channel = self.connection.channel()

    def add_queues(self, *args):
        for queue_name in args:
            self.channel.queue_declare(queue=queue_name)

    def listen(self):
        self.channel.start_consuming()

    def send_to_queue(self, queue_name: str, message: Message):
        content_info = [
            f"{key}: {len(str(message.get(key)))} Bytes" for key in message.keys()
        ]
        logging.debug(f"Message to: {queue_name} | {content_info}")
        self.channel.basic_publish(
            exchange="", routing_key=queue_name, body=message.marshal()
        )

    def broadcast_to_group(self, group_name: str, message: Message):
        self.channel.basic_publish(
            exchange=group_name, routing_key="", body=message.marshal()
        )

    def set_callback(self, queue_name: str, callback: Callable, args: tuple = ()):
        custom_callback = lambda ch, method, properties, body: callback(
            self, Message.unmarshal(body.decode()), *args
        )
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=custom_callback, auto_ack=True
        )

    def add_broadcast_group(self, group_name: str, queue_names: list[str]):
        self.channel.exchange_declare(exchange=group_name, exchange_type="fanout")
        for queue_name in queue_names:
            self.channel.queue_bind(exchange=group_name, queue=queue_name)

    def close(self):
        self.channel.close()
        if self.connection.is_open:
            try:
                self.connection.close()
            except Exception as e:
                # Already closed by another Node
                pass

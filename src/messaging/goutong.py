"""
Goutong is a middleware that abstracts MOM (Message Oriented Middleware) for the system.
It uses RabbitMQ.
"""

import logging
import pika
from typing import Callable
from .message import Message


class Goutong:
    def __init__(self, host: str = "rabbit", port: int = 5672):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port)
        )
        self.channel = self.connection.channel()
        self.consumer_ids = {}

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

    def set_callback(
        self, queue_name: str, callback: Callable, auto_ack=True, args: tuple = ()
    ):
        custom_callback = lambda ch, method, properties, body: callback(
            self, Message.unmarshal(body.decode()).with_id(method.delivery_tag).from_queue(queue_name), *args
        )

        consumer_id = self.channel.basic_consume(
            queue=queue_name, on_message_callback=custom_callback, auto_ack=auto_ack
        )

        # logging.debug(f"ID: {consumer_id} | Listening to: {queue_name}")
        self.consumer_ids[queue_name] = consumer_id

    def stop_consuming(self, queue_name: str):
        self.channel.basic_cancel(self.consumer_ids[queue_name])

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

    def ack_all_messages(self):
        self.channel.basic_ack(multiple=True)

    def ack_delivery(self, delivery_id: int):
        self.channel.basic_ack(delivery_tag=delivery_id)

    def nack_delivery(self, delivery_id: int):
        self.channel.basic_nack(delivery_tag=delivery_id)

"""
Goutong is a middleware that abstracts MOM (Message Oriented Middleware) for the system.
It uses RabbitMQ.
"""

import logging
import pika
from typing import Callable, Optional
from .message import Message
from pika.exceptions import AMQPConnectionError, AMQPChannelError

class Goutong:
    def __init__(self, sender_id: str, host: str = "rabbit", port: int = 5672):
        self.sender_id = sender_id
        self.queues_added = set()
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port)
        )
        self.channel = self.connection.channel()
        self.consumer_ids = {}
        self.shutting_down = False

    # def add_queues(self, *args):
    #     self.queues_added.update(args)
    #     for queue_name in args:
    #         self.channel.queue_declare(queue=queue_name)

    def delete_queue(self, queue_name: str):
        self.channel.queue_delete(queue_name)

    def listen(self):
        try:
            self.channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError, IndexError):
            if self.shutting_down:
                pass

    def send_to_queue(self, queue_name: str, message: Message, flow_id: Optional[str] =None):
        
        sender_id = self.sender_id
        if flow_id:
            sender_id += f"@{flow_id}"
        
        message = message.with_sender(sender_id)

        if queue_name not in self.queues_added:
            self.queues_added.add(queue_name)
            self.channel.queue_declare(queue=queue_name)
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
        if queue_name not in self.queues_added:
            self.queues_added.add(queue_name)
            self.channel.queue_declare(queue=queue_name)

        custom_callback = lambda ch, method, properties, body: callback(
            self, Message.unmarshal(body.decode()).with_id(method.delivery_tag).from_queue(queue_name), *args
        )

        consumer_id = self.channel.basic_consume(
            queue=queue_name, on_message_callback=custom_callback, auto_ack=auto_ack
        )

        self.consumer_ids[queue_name] = consumer_id

    def stop_consuming(self, queue_name: str):
        self.channel.basic_cancel(self.consumer_ids[queue_name])

    def add_broadcast_group(self, group_name: str, queue_names: list[str]):
        for q in queue_names:
            if q not in self.queues_added:
                self.queues_added.add(q)
                self.channel.queue_declare(q)

        self.channel.exchange_declare(exchange=group_name, exchange_type="fanout")
        for queue_name in queue_names:
            self.channel.queue_bind(exchange=group_name, queue=queue_name)

    def close(self):
        self.shutting_down = True
        try:
            self.channel.close()
        except Exception as e:
            # Could be in a ChannelError state
            pass
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

    def requeue(self, message: Message):
        queue_name = message.queue_name
        self.channel.basic_publish(
            exchange="", routing_key=queue_name, body=message.marshal()
        )
        logging.debug(f"Requeued message {message.get('sender')}#{message.get('transaction_id')} to: {queue_name}")
        self.ack_delivery(message.delivery_id)

    # def nack_delivery(self, delivery_id: int):
    #     self.channel.basic_nack(delivery_tag=delivery_id, requeue=False, multiple=True)

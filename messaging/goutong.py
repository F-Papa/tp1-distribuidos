"""
Goutong is a middleware that abstracts MOM (Message Oriented Middleware) for the system.
It uses RabbitMQ.
"""

import pika
from typing import Callable
from messaging.message import Message

class Goutong():
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbit"))
        self.channel = connection.channel()

    def add_queues(self, *args):
        for queue_name in args:
            self.channel.queue_declare(queue=queue_name)

    def listen(self):
        self.channel.start_consuming()

    def send_to_queue(self, queue_name: str, message: Message):
        self.channel.basic_publish(exchange='',
                                  routing_key=queue_name,
                                  body=message.marshal())

    def set_callback(self, queue_name: str, callback: Callable, args: tuple = ()):
        custom_callback = lambda ch, method, properties, body: callback(self, Message.unmarshal(body.decode()), *args) 
        self.channel.basic_consume(queue=queue_name, on_message_callback=custom_callback, auto_ack=True)
from enum import Enum
import logging
import socket
import sys

MSG_REDUNDANCY = 3
PORT = 12347
BYTES_SIZE = 2


class Message(Enum):
    HEALTHCHECK = 6
    IM_ALIVE = 7


def crash_maybe():
    import random

    if random.random() < 0.01:
        logging.error("Crashing...")
        sys.exit(1)


class ControlMessage(Enum):
    HEALTHCHECK = 6
    IM_ALIVE = 7


def healthcheck_handler(controller):
    logging.debug("Healthcheck thread started")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", PORT))

    last_seq_num = None
    last_medic_id = None

    try:
        while not controller.is_shutting_down():
            seq_num, sender_addr, sender_id, message = receive_message(sock)

            # Ignore redundant messages
            if last_seq_num and last_seq_num == seq_num:
                if last_medic_id and last_medic_id == sender_id:
                    continue

            last_seq_num = seq_num
            last_medic_id = sender_id

            if message == ControlMessage.HEALTHCHECK.value:
                controller.ack_unacknowledged_messages()
                # logging.info("Sending healthcheck response...")
                send_im_alive(
                    recv_addr=sender_addr[0],
                    sender_id=controller.controller_id(),
                    seq_num=seq_num,
                    sock=sock,
                )
            else:
                logging.error(f"Unexpected message received: {message.value}")

    except Exception as e:
        logging.error(e)
    finally:
        sock.close()

    logging.info("Healthcheck thread stopped")


def send_im_alive(recv_addr, sender_id, seq_num, sock):
    message = f"{seq_num},{sender_id},{ControlMessage.IM_ALIVE.value}"
    length = len(message) + BYTES_SIZE
    encoded_length = length.to_bytes(BYTES_SIZE, byteorder="big")
    message = encoded_length + message.encode()

    for _ in range(MSG_REDUNDANCY):
        crash_maybe()
        send_message(message, recv_addr, PORT, sock)


def send_message(message: bytes, recv_address: str, recv_port: int, sock):
    bytes_sent = 0
    while bytes_sent < len(message):
        bytes_sent += sock.sendto(message[bytes_sent:], (recv_address, recv_port))


def receive_message(sock: socket.socket) -> tuple[int, str, str, Message]:
    """Seq_num, Address, controller_id, message_code"""
    data = bytes()

    while len(data) < BYTES_SIZE:
        received, address = sock.recvfrom(BYTES_SIZE - len(data))
        data += received

    size_to_read = int.from_bytes(data, "big")
    data = bytes()

    while len(data) < size_to_read:
        crash_maybe()
        received, address = sock.recvfrom(size_to_read - len(data))
        data += received

    seq_num, controller_id, response_code = data[BYTES_SIZE:].decode().split(",")
    seq_num = int(seq_num)
    response_code = int(response_code)

    return seq_num, address[0], controller_id, Message(response_code)

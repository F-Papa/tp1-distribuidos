from enum import Enum
import logging
import socket

MSG_REDUNDANCY = 3
CONTROL_PORT = 12347


class ControlMessage(Enum):
    HEALTHCHECK = 6
    IM_ALIVE = 7


def healthcheck_handler(controller):
    logging.info("Healthcheck thread started")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", CONTROL_PORT))
    terminator_bytes = bytes("$", "utf-8")[0]

    last_seq_num = None
    last_medic_id = None

    try:
        while not controller.is_shutting_down():
            data = b""
            while len(data) == 0 or data[-1] != terminator_bytes:
                try:
                    recieved, address = sock.recvfrom(1024)
                    data += recieved
                except socket.timeout:
                    break

            data = data.decode()
            logging.debug(f"received healthcheck: {data}")
            seq_num, medic_id, response_code = data[:-1].split(",")
            seq_num = int(seq_num)
            response_code = int(response_code)

            # Ignore redundant messages
            if last_seq_num and last_seq_num == seq_num:
                if last_medic_id and last_medic_id == medic_id:
                    continue

            last_seq_num = seq_num
            last_medic_id = medic_id

            if response_code == ControlMessage.HEALTHCHECK.value:
                send_healthcheck_response(
                    recv_address=address[0],
                    recv_name=medic_id,
                    sender_id=controller.controller_id(),
                    seq_num=seq_num,
                )
            else:
                logging.error(f"Unexpected message received: {data}")

    except Exception as e:
        logging.error(e)
    finally:
        sock.close()

    logging.info("Healthcheck thread stopped")


def send_healthcheck_response(recv_address, recv_name, sender_id, seq_num):
    message = f"{seq_num},{sender_id},{ControlMessage.IM_ALIVE.value}$"
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logging.info(f"Sending IM ALIVE to: {recv_address} ({recv_name})")

    for _ in range(MSG_REDUNDANCY):
        sock.sendto(message.encode(), (recv_address, CONTROL_PORT))

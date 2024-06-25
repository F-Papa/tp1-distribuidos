import socket
import threading
import logging
from enum import Enum
from typing import Optional


class Message(Enum):
    HEALTHCHECK = 7
    IM_ALIVE = 8

class HealthcheckHandler:
    CONNECTION_PORT = 12345
    INT_ENCODING_LENGTH = 1
    
    def __init__(
        self,
        controller
    ):
        self.controller = controller

    def start(self):
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.bind(('0.0.0.0', self.CONNECTION_PORT))
                    received, addr = s.recvfrom(1024)
                    if self.is_type(received, Message.HEALTHCHECK):
                        medic_id = self.sender_id(received)
                        if not medic_id:
                            logging.error(f"Received 👨‍⚕️ healthcheck from unknown medic")
                        if len(threading.enumerate()) == 2: # Check if main Thread is alive
                            response = self.im_alive_msg()
                            #logging.info(f"SENDING{response}")
                            s.sendto(response, (medic_id, self.CONNECTION_PORT))
                            #logging.debug(f"Sent 🤑 IM_ALIVE response to {medic_id}")
                        time_passed_func = getattr(self.controller, "time_window_passed", None)
                        if callable(time_passed_func):
                            logging.info(f"TIME WDW PASSED!!")
                            self.controller.time_window_passed()
            except Exception as e:
                logging.error(f"Error in start method: {e}")
                break
        logging.info(f"Exiting.")

    def decode_int(self, bytes: bytes) -> int:
        return int.from_bytes(bytes, "big")

    def is_type(self, data: bytes, msg_type: Message) -> bool:
        return self.decode_int(data[:self.INT_ENCODING_LENGTH]) == msg_type.value

    def healthcheck_msg(self, sender_id: str) -> bytes:
        return (
            b""
            + Message.HEALTHCHECK.value.to_bytes(self.INT_ENCODING_LENGTH, "big")
            + len(sender_id).to_bytes(self.INT_ENCODING_LENGTH, "big")
            + sender_id.encode('utf-8')
        )

    def im_alive_msg(self) -> bytes:
        return (
            b""
            + Message.IM_ALIVE.value.to_bytes(self.INT_ENCODING_LENGTH, "big")
            + len(self.controller.controller_id()).to_bytes(self.INT_ENCODING_LENGTH, "big")
            + self.controller.controller_id().encode('utf-8')
        )

    def sender_id(self, received: bytes) -> Optional[str]:
        length = self.decode_int(received[self.INT_ENCODING_LENGTH:2*self.INT_ENCODING_LENGTH])
        id = received[self.INT_ENCODING_LENGTH*2:].decode("utf-8")
        if len(id) != length:
            logging.error(f"Incomplete sender_id: {id} | {length} {len(id)}")
            return None
        return id
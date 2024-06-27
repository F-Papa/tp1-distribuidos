import socket
import threading
import logging
from enum import Enum
from typing import Any, Optional


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
        self._shutting_down = False
        self._sock: Any = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(('0.0.0.0', self.CONNECTION_PORT))

    def start(self):
        while not self._shutting_down:
            try:
                received, addr = self._sock.recvfrom(1024)
                if self.is_type(received, Message.HEALTHCHECK) and not self._shutting_down:
                    medic_id = self.sender_id(received)
                    if not medic_id:
                        logging.error(f"Received 👨‍⚕️ healthcheck from unknown medic")
                    if len(threading.enumerate()) == 2: # Check if controller Thread is alive
                        response = self.im_alive_msg()
                        if not self._shutting_down:
                            self._sock.sendto(response, (medic_id, self.CONNECTION_PORT))
                    else:
                        exit(0)
            except Exception:
                if self._shutting_down:
                    pass
        if self._sock:
            self._sock.close()        
            
    def shutdown(self):     
        logging.info("SIGTERM received. Initiating Graceful Shutdown.")
        self._shutting_down = True
        self.controller.shutdown()
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            self._sock = None
        except OSError:
            pass


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
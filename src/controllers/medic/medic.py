import random
import signal
import socket
import time
import logging
import docker
from enum import Enum
import os
import logging
import threading
from src.utils.config_loader import Configuration


HEALTHCHECK_REQUEST_CODE = 1
HEALTHCHECK_RESPONSE_CODE = 2


class Message(Enum):
    ELECTION = 3
    OK = 4
    COORDINATOR = 5
    HEALTHCHECK = 6
    IM_ALIVE = 7


PORT_1 = 12347
PORT_2 = 12348



TIMEOUT = 2
VOTING_DURATION = 8
LEADER_ANNOUNCEMENT_DURATION = 8
HEALTH_CHECK_INTERVAL_SECONDS = 15
VOTING_COOLDOWN = 6
MSG_REDUNDANCY = 1


class Medic:
    CONTROLLER_TYPE = "medic"

    def __init__(self, controllers_to_check: dict, config: Configuration):
        # dict{nombre_controller: address_controller}
        self._number_of_medics = config.get("NUMBER_OF_MEDICS")
        self._medic_number = config.get("MEDIC_NUMBER")
        self._other_medics = {}
        self._bigger_medics = {}
        
        for i in range(1, self._number_of_medics + 1):
            if i > self._medic_number:
                self._bigger_medics[f"medic{i}"] = f"medic{i}"
            if i != self._medic_number:
                self._other_medics[f"medic{i}"] = f"medic{i}"
    
        self.other_controllers = controllers_to_check.copy()
        self.controllers_to_check = {}
        
        self._seq_num = 0
        self._id = f"{self.CONTROLLER_TYPE}{self._medic_number}"
        self._is_leader = False
        self._leader = None
        self.docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")

        self._sock_1: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock_2: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock_1.bind(("0.0.0.0", PORT_1))
        self._sock_2.bind(("0.0.0.0", PORT_2))
        
        self._election_condvar = threading.Condition()
        self._coordinator_condvar = threading.Condition()
        self._coordinator_received = False
        self._ok_condvar = threading.Condition()
        self._election_related_condvar = threading.Condition()

        self._ok_received = False

        self._coordinator_received = None
        self._election_pending = True

    def is_leader(self):
        return self._is_leader

    def hostname(self, receiver_id: str):
        return receiver_id

    def send_message(self, message: str, receiver_id: str, port: int, sock: socket.socket):
        name =  Message(int(message.split(",")[-1])).name
        # logging.info(f"Sending to {receiver_id}: {name}")
        try:
            for _ in range(MSG_REDUNDANCY):
                sock.sendto(message.encode(), (self.hostname(receiver_id), port))
        except socket.gaierror:
            pass

    def recv_message(self, sock: socket.socket) -> tuple[int, str, Message]:
        received, address = sock.recvfrom(1024)
        seq_num, sender_id, message_code = received.decode().split(",")
        return int(seq_num), sender_id, Message(int(message_code))

    def check_on_leader(self):
        logging.info(f"Checking on Leader")
        
        # for controller_id, address in self.controllers_to_check.items():
        if not self._leader:
            logging.error("Im neither leader nor follower")
            exit(1)

        message = f"{self._seq_num},{self._id},{Message.HEALTHCHECK.value}"
        self.send_message(message, self._leader, PORT_2, self._sock_1)

        im_alives = set()
        end_of_check = time.time() + HEALTH_CHECK_INTERVAL_SECONDS
        
        while time.time() < end_of_check:
            timeout = end_of_check - time.time()
            self._sock_1.settimeout(timeout)
            try:
                (seq_num, sender_id, msg) = self.recv_message(self._sock_1)
                if msg == Message.IM_ALIVE and sender_id == self._leader:
                    im_alives.add(sender_id)
            except socket.timeout:
                logging.info(f"Done listening for IM_ALIVES, received: {im_alives}")
        
        if not im_alives:
            logging.error("Leader is dead!")
            self.handle_elections()

        logging.info(f"Finished checking on Leader")
        
    

    def revive_controller(self, controller_name):
        logging.info(f"REVIVING CONTROLLER {controller_name}")
        container = self.docker_client.containers.get(controller_name)
        try:
            container.kill()
        except:
            pass
        container.start()

    # RECIBE "ELECTIONS" POR PORT 2, CONTESTA "OK" AL PORT 1. 
    # RECIBE "HEALTHCHECK" POR PORT 2, CONTESTA "IMALIVE" AL PORT 1.
    def listen_thread(self):
        while True:
            (seq_num, sender_id, msg) = self.recv_message(self._sock_2)
            if msg == Message.ELECTION:
                
                ok = f"{self._seq_num},{self._id},{Message.OK.value}"
                self.send_message(ok, sender_id, PORT_2, self._sock_2)
                
                if not self._election_pending:
                    election = f"{self._seq_num},{self._id},{Message.ELECTION.value}"
                    for m in self._bigger_medics:
                        self.send_message(election, m, PORT_2, self._sock_2)
                    
                with self._election_condvar:
                    self._election_condvar.notify()

            elif msg == Message.HEALTHCHECK:
                ok = f"{self._seq_num},{self._id},{Message.IM_ALIVE.value}"
                self.send_message(ok, sender_id, PORT_1, self._sock_2)
            
            elif msg == Message.OK:
                self._ok_received = True

            elif msg == Message.COORDINATOR:
                with self._coordinator_condvar:
                    logging.info(f"👌   New Leader: {sender_id}")
                    self._leader = sender_id
                    self._is_leader = False
                    self._coordinator_received = True
                    self._coordinator_condvar.notify()

    # MANDA "ELECTION" A PORT 2, ESPERA "OK" y "COORDINATOR" POR PORT 1
    def handle_elections(self):
        self._election_pending = True
        # Election which Im a part of
        logging.info("Voting 🗳️")
        
        voting_end = time.time() + VOTING_DURATION
        announcement_end = voting_end + LEADER_ANNOUNCEMENT_DURATION
        
        # Start election
        self._ok_received = False
        for bigger_medic in self._bigger_medics:
            election = f"{self._seq_num},{self._id},{Message.ELECTION.value}"
            self.send_message(election, bigger_medic, PORT_2, self._sock_1)
            self._seq_num += 1
       
        with self._coordinator_condvar:
            timeout_1 = max(0, voting_end - time.time())
            
            if self._coordinator_condvar.wait_for(lambda: self._coordinator_received, timeout_1):
                # LISTEN THREAD RECEIVED COORDINATOR during wait(), ELECTION ENDS.
                self._election_pending = False
                self._coordinator_received = False
                return

            # VOTING TIME ENDED
            if not self._ok_received:
                # NO GREATER MEDIC REPLIED OK, IM THE LEADER
                self._leader = self._id
                self._is_leader = True
                self._election_pending = False
                self.announce_leader()
                return

            else:
                # AT LEAST ONE GREATER MEDIC ANSWERED OK
                # LISTEN FOR COORDINATORS         
                self._ok_received = False      
                timeout_2 = max(0, announcement_end - time.time())
                if self._coordinator_condvar.wait_for(lambda: self._coordinator_received, timeout_2):
                    # LISTEN THREAD RECEIVED COORDINATOR, ELECTION ENDS.
                    self._coordinator_received = False
                    self._election_pending = False
                    return
                
                # ANNOUNCEMENT TIME ENDED WITHOUT COORDINATOR
                # RETURN AND START NEW ELECTION
                self._leader = None
                self._is_leader = False

        self._election_pending = False

    # region: Old
    # def handle_elections(self):
    #     # Election decided by Medics with bigger ID
    #     with self._election_condvar:
    #         if self._coordinator_received:
    #             (seq_num, sender_id, msg) = self._coordinator_received
    #             self._coordinator_received = None
    #             if sender_id in self._bigger_medics:
    #                 self.set_other_leader(sender_id)
    #                 self._election_pending = False
    #                 return

    #     # Election which Im a part of
    #     logging.info("Starting election")
    #     for bigger_medic in self._bigger_medics:
    #         election = f"{self._seq_num},{self._id},{Message.ELECTION.value}"
    #         self.send_message(election, bigger_medic, PORT_2, self._sock_1)
    #         self._seq_num += 1

    #     oks_received = set()
    #     end_of_voting = time.time() + VOTING_DURATION

        
    #     # Listen for OKs
    #     logging.info("Listening for OKs")
    #     while time.time() < end_of_voting:
    #         timeout = end_of_voting - time.time()
    #         self._sock_1.settimeout(timeout)
    #         try:
    #             (seq_num, sender_id, msg) = self.recv_message(self._sock_1)
    #             if msg == Message.OK:
    #                 oks_received.add(sender_id)
    #         except socket.timeout:
    #             logging.info(f"Done listening for OKS, received: {oks_received}")

    #     # Check OKs Received
    #     if oks_received:
    #         end_of_announcement = time.time() + LEADER_ANNOUNCEMENT_DURATION
    #         while time.time() < end_of_announcement:
    #             timeout = end_of_announcement - time.time()
    #             with self._election_condvar:
    #                 if not self._coordinator_received:
    #                     self._election_condvar.wait(timeout)
                    
    #                 if not self._coordinator_received:
    #                     logging.error("Me mintieron")
    #                     self._election_pending = False
    #                     return
                    
    #                 (seq_num, sender_id, msg) = self._coordinator_received
    #                 self._coordinator_received = None
    #                 if msg == Message.COORDINATOR:
    #                     if sender_id in self._bigger_medics:
    #                         self.set_other_leader(sender_id)
    #                         self._election_pending = False
    #                         return
    #                     else:
    #                         logging.error(f"Received coordinator from {sender_id} | Bigger: {sender_id in self._bigger_medics}, OK Received: {sender_id in oks_received}")
    #     else:
    #         self._leader = self._id
    #         self._is_leader = True
    #         self.announce_leader()
    #         self._election_pending = False
    # endregion

    
    
    def announce_leader(self):
        logging.info(f"⭐   I'm the leader ({self._id})")
        for medic in self._other_medics:
            coord = f"{self._seq_num},{self._id},{Message.COORDINATOR.value}"
            self.send_message(coord, medic, PORT_2, self._sock_1)

    def set_other_leader(self, other: str):
        self._leader = other
        logging.info(f"The leader is ({other})")

    def start(self):
        threading.Thread(target=self.listen_thread, args=()).start()
        time.sleep(2)
        
        while True:
            self.handle_elections()
            if not self._is_leader and self._leader is None:
                time.sleep(VOTING_COOLDOWN)
                continue
            
            with self._election_condvar:
                if not self._election_condvar.wait(timeout=random.randrange(20, 30)):
                    logging.info("📣    Raising Election")
  
            # if self._is_leader:
            #     pass
            # else:
            #     self.check_on_leader()

def config_logging(level: str):

    level = getattr(logging, level)

    # Filter logging
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():

    required = {
        "LOGGING_LEVEL": str,
        "MEDIC_NUMBER": int,
        "NUMBER_OF_MEDICS": int,
    }

    config = Configuration.from_file(required, "config.ini")
    config.update_from_env()
    config.validate()

    config_logging(config.get("LOGGING_LEVEL"))
    logging.info(config)

    medic = Medic(
        config=config, controllers_to_check={}
    )

# "title_filter1": "title_filter1", "title_filter2": "title_filter2",
#                                              "title_filter_proxy": "title_filter_proxy", "date_filter1": "date_filter1",
#                                                "date_filter2": "date_filter2", "date_filter_proxy": "date_filter_proxy",
#                                                "category_filter_proxy": "category_filter_proxy", "category_filter1": "category_filter1"}

    medic.start()


if __name__ == "__main__":
    main()

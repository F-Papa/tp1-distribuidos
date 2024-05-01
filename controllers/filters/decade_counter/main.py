import json
import os
from typing import Union
from messaging.goutong import Goutong
from messaging.message import Message
import logging
import signal

from utils.config_loader import Configuration
from exceptions.shutting_down import ShuttingDown

INPUT_QUEUE = "decade_counter_queue"
FILTER_TYPE = "decade_counter"
CONTROL_GROUP = "CONTROL"

OUTPUT_Q2 = "results_queue"

shutting_down = False


# Graceful Shutdown
def sigterm_handler(messaging: Goutong):
    global shutting_down
    logging.info("SIGTERM received. Initiating Graceful Shutdown.")
    shutting_down = True
    msg = Message({"ShutDown": True})
    # messaging.broadcast_to_group(CONTROL_GROUP, msg)


def config_logging(level: str):

    level = getattr(logging, level)

    # Filter logging
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Hide pika logs
    pika_logger = logging.getLogger("pika")
    pika_logger.setLevel(logging.ERROR)


class AuthorCache:
    # Q2_FILE = "q2_authors.json"
    FILE_PREFIX = "authors"
    FILE_SUFFIX = ".json"
    KEY_VALUE_SEPARATOR = "%%%"
    N_PARTITIONS = 500

    def __init__(self, cache_vacants: int) -> None:
        self.cache: dict[str, set[int]] = {}
        self.cache_vacants = cache_vacants
        self.cached_entries = 0
        self.entries_in_files = 0
        # create files from scratch
        for i in range(self.N_PARTITIONS):
            file_name = f"{self.FILE_PREFIX}_{i}{self.FILE_SUFFIX}"
            with open(file_name, "w") as f:
                pass

    def get_10_decade_authors(self) -> list[str]:
        valid_author = lambda author: repr(author) != "''" and not author.isspace()

        ten_decade_authors = list(
            filter(
                lambda author: (len(self.cache[author]) >= 10) and valid_author(author),
                self.cache.keys(),
            )
        )

        if self.entries_in_files > 0:
            for i in range(self.N_PARTITIONS):
                file_name = f"{self.FILE_PREFIX}_{i}{self.FILE_SUFFIX}"
                with open(file_name, "r") as f:
                    for line in f:

                        author = line.split(self.KEY_VALUE_SEPARATOR)[0]
                        decades = json.loads(line.split(self.KEY_VALUE_SEPARATOR)[1])
                        if len(decades) >= 10 and valid_author(author):
                            ten_decade_authors.append(author)

        return ten_decade_authors

    def n_elements_in_cache(self) -> int:
        return self.cached_entries

    def _write_oldest_to_disk(self):
        if len(self.cache) == 0:
            raise ValueError("Cache is empty")

        first_author_in_cache: str = list(self.cache.keys())[0]
        author_decades = list(self.cache.pop(first_author_in_cache))

        partition = hash(first_author_in_cache) % self.N_PARTITIONS
        file_name = f"{self.FILE_PREFIX}_{partition}{self.FILE_SUFFIX}"
        entry = f"{first_author_in_cache}{self.KEY_VALUE_SEPARATOR}{json.dumps(author_decades)}\n"
        with open(file_name, "a") as f:
            f.write(entry)

        self.entries_in_files += 1
        self.cached_entries -= 1

    def _pop_from_disk(self, author: str) -> tuple[str, Union[set[int], None]]:
        temp_file_name = "temp.json"

        partition = hash(author) % self.N_PARTITIONS
        file_name = f"{self.FILE_PREFIX}_{partition}{self.FILE_SUFFIX}"
        value_from_disk: set[int] | None = None
        with open(file_name, "r") as original_file, open(
            temp_file_name, "w"
        ) as temp_file:
            for line in original_file:
                sep_index = line.find(self.KEY_VALUE_SEPARATOR)
                line_author = line[:sep_index]
                if line_author != author:
                    temp_file.write(line)
                else:
                    aux = json.loads(line[sep_index + len(self.KEY_VALUE_SEPARATOR) :])
                    value_from_disk = set()
                    value_from_disk.update(aux)
                    self.entries_in_files -= 1

        os.replace(temp_file_name, file_name)

        return (author, value_from_disk)

    def add(self, author: str, decade: int):
        dbg_string = "Adding (%s) | Cache Used: %d/%d | Entries in files: %d" % (
            # author[0:10] + "...",
            author,
            self.n_elements_in_cache(),
            self.cache_vacants,
            self.entries_in_files,
        )
        logging.debug(dbg_string)

        # Already cached, add the new decades and return
        if author in self.cache.keys():
            self.cache[author].add(decade)
            return

        decades_in_disk = None
        # Could be in file
        if self.entries_in_files > 0:
            _, decades_in_disk = self._pop_from_disk(author)

        # If it is in the disk, add the new decade to the existing ones, otherwise create a new set
        author_decades = decades_in_disk if decades_in_disk is not None else set()
        author_decades.add(decade)

        # If the cache is full, write the oldest entry to disk
        if self.n_elements_in_cache() >= self.cache_vacants:
            logging.debug(f"Committing 1 entry to disk")
            self._write_oldest_to_disk()

        # Add the author to the cache, whether it was in the disk or a new one
        self.cached_entries += 1
        self.cache.update({author: author_decades})

    def get(self, author: str) -> Union[set[int], None]:
        if author in self.cache.keys():
            return self.cache[author]

        elif self.entries_in_files > 0:
            _, decades = self._pop_from_disk(author)
            if decades is not None:
                self.cache.update({author: decades})
            return decades
        else:
            return None


def main():
    required = {
        "LOGGING_LEVEL": str,
        "CACHE_VACANTS": int,
    }

    filter_config = Configuration.from_file(required, "config.ini")
    filter_config.update_from_env()
    filter_config.validate()

    config_logging(filter_config.get("LOGGING_LEVEL"))
    logging.info(filter_config)

    authors_cache = AuthorCache(filter_config.get("CACHE_VACANTS"))
    messaging = Goutong()

    # Set up the queues
    control_queue_name = FILTER_TYPE + "_control"
    own_queues = [INPUT_QUEUE, control_queue_name, OUTPUT_Q2]
    messaging.add_queues(*own_queues)

    messaging.add_broadcast_group(CONTROL_GROUP, [control_queue_name])
    messaging.set_callback(control_queue_name, callback_control, ())

    messaging.set_callback(INPUT_QUEUE, callback_filter, (filter_config, authors_cache))

    signal.signal(signal.SIGTERM, lambda sig, frame: sigterm_handler(messaging))

    # Start listening
    if not shutting_down:
        try:
            messaging.listen()
        except ShuttingDown:
            logging.debug("Shutdown Message Received via Control Broadcast")

    messaging.close()
    logging.info("Shutting Down.")


def callback_control(messaging: Goutong, msg: Message):
    global shutting_down
    if msg.has_key("ShutDown"):
        shutting_down = True
        raise ShuttingDown


def _send_EOF(messaging: Goutong):
    msg = Message({"EOF": True})
    messaging.send_to_queue(OUTPUT_Q2, msg)
    logging.debug(f"Sent EOF to: {OUTPUT_Q2}")


def callback_filter(
    messaging: Goutong,
    msg: Message,
    config: Configuration,
    decades_per_author: AuthorCache,
):

    if msg.has_key("EOF"):
        logging.debug("Received EOF")
        # Forward EOF and Keep Consuming
        _send_results_q2(messaging, decades_per_author)
        _send_EOF(messaging)
        return

    books = msg.get("data")
    # logging.debug(f"Received {len(books)} books")
    for book in books:
        decade = book.get("decade")
        # Query 2 flow
        for author in book.get("authors"):
            decades_per_author.add(author, decade)

    logging.debug(
        f"Authors: {decades_per_author.cached_entries + decades_per_author.entries_in_files}"
    )


def _send_results_q2(messaging: Goutong, author_cache: AuthorCache):
    ten_decade_authors = author_cache.get_10_decade_authors()
    msg = Message({"query": 2, "data": ten_decade_authors})
    messaging.send_to_queue(OUTPUT_Q2, msg)
    logging.debug(f"Sent Data to: {OUTPUT_Q2}")


if __name__ == "__main__":
    main()

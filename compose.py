TITLE_FILTER_COUNT = 4
DATE_FILTER_COUNT = 3
CATEGORY_FILTER_COUNT = 3
ITEMS_PER_BATCH = 100
BARRIER_LOGGING_LEVEL = "INFO"


def header_text():
    return """\
version: '3.9'
name: 'filters'
services:
"""


def footer_text():
    return """\
networks:
  tp1_testing_net:
    external: true
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""


def barrier_service_text(filter_type: str, filter_count: int, logging_level: str):

    return f"""\
  {filter_type}_barrier:
    build:
      context: ./
      dockerfile: ./controllers/barrier/Dockerfile
    container_name: {filter_type}_barrier
    image: barrier:latest
    entrypoint: python3 main.py
    environment:
      - FILTER_TYPE={filter_type}
      - FILTER_COUNT={filter_count}
      - LOGGING_LEVEL={logging_level}
    networks:
      - tp1_testing_net
    volumes:
      - ./controllers/barrier/main.py:/main.py
"""


def date_filter_service_text(number: int, items_per_batch: int):
    return f"""\
  date_filter{number}:
    build:
      context: ./
      dockerfile: ./controllers/filters/date_filter/Dockerfile
    container_name: date_filter{number}
    image: date_filter:latest
    entrypoint: python3 main.py
    environment:
      - ITEMS_PER_BATCH={items_per_batch}
      - FILTER_NUMBER={number}
    networks:
      - tp1_testing_net
    volumes:
      - ./controllers/filters/date_filter/config.ini:/config.ini
      - ./controllers/filters/date_filter/main.py:/main.py
"""


def category_filter_service_text(number: int, items_per_batch: int):
    return f"""\
  category_filter{number}:
    build:
      context: ./
      dockerfile: ./controllers/filters/category_filter/Dockerfile
    container_name: category_filter{number}
    image: category_filter:latest
    entrypoint: python3 main.py
    environment:
      - ITEMS_PER_BATCH={items_per_batch}
      - FILTER_NUMBER={number}
    networks:
      - tp1_testing_net
    volumes:
      - ./controllers/filters/category_filter/config.ini:/config.ini
      - ./controllers/filters/category_filter/main.py:/main.py
"""


def title_filter_service_text(number: int, items_per_batch: int):
    return f"""\
  title_filter{number}:
    build:
      context: ./
      dockerfile: ./controllers/filters/title_filter/Dockerfile
    container_name: title_filter{number}
    image: title_filter:latest
    entrypoint: python3 main.py
    environment:
      - ITEMS_PER_BATCH={items_per_batch}
      - FILTER_NUMBER={number}
    networks:
      - tp1_testing_net
    volumes:
      - ./controllers/filters/title_filter/config.ini:/config.ini
      - ./controllers/filters/title_filter/main.py:/main.py
"""


if __name__ == "__main__":
    # Create the docker-compose.yml file
    with open("docker-compose.yml", "w") as f:

        # Write the header
        f.write(header_text())
        f.write("\n")

        # Title filters & barrier
        f.write(
            barrier_service_text(
                "title_filter", TITLE_FILTER_COUNT, BARRIER_LOGGING_LEVEL
            )
        )
        f.write("\n")

        for i in range(1, TITLE_FILTER_COUNT + 1):
            f.write(title_filter_service_text(i, ITEMS_PER_BATCH))
            f.write("\n")

        # Date filters & barrier
        f.write(
            barrier_service_text(
                "date_filter", DATE_FILTER_COUNT, BARRIER_LOGGING_LEVEL
            )
        )
        f.write("\n")

        for i in range(1, DATE_FILTER_COUNT + 1):
            f.write(date_filter_service_text(i, ITEMS_PER_BATCH))
            f.write("\n")

        # Category filters & barrier
        f.write(
            barrier_service_text(
                "category_filter", CATEGORY_FILTER_COUNT, BARRIER_LOGGING_LEVEL
            )
        )
        f.write("\n")

        for i in range(1, CATEGORY_FILTER_COUNT + 1):
            f.write(category_filter_service_text(i, ITEMS_PER_BATCH))
            f.write("\n")

        # Write the footer
        f.write(footer_text())

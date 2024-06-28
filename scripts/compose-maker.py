"""
compose-maker.py

Descripción:
    Este script genera un archivo docker-compose.yml en el root del proyecto
    con la configuración que se elija según las constantes de este archivo.


Uso:
    *** IMPORTANTE ***
    Ejecute este script desde el directorio raíz del proyecto.
    > python3 scripts/compose-maker.py
"""

TITLE_FILTER_COUNT = 3
DATE_FILTER_COUNT = 2
CATEGORY_FILTER_COUNT = 4
DECADE_COUNTER_COUNT = 3
REVIEW_JOINER_COUNT = 3
REVIEW_COUNTER_COUNT = 3
SENTIMENT_ANALYZER_COUNT = 6
SENTIMENT_AVERAGER_COUNT = 2
MEDIC_COUNT = 4
ITEMS_PER_BATCH = 700
BARRIER_LOGGING_LEVEL = "INFO"

CRASH_CONTROLLERS_BY_DEFAULT = 1 #If 1 controllers will sporadically fail, each individual controller can be set manually to either true or false in the compose file
                                 #If 0 controllers won't sporadically fail

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

def boundary_service_text():
    return """\
  boundary:
    build:
      context: ./
      dockerfile: ./src/boundary/Dockerfile
    container_name: boundary
    image: boundary:latest
    entrypoint: python3 boundary.py
    networks:
      - tp1_testing_net
    ports:
      - "8080:8080"
    volumes:
      - ./src/boundary/config.ini:/config.ini
      - ./src/boundary/boundary.py:/boundary.py
"""

def proxy_service_text(filter_type: str, input_queues: str, key_to_hash: str, filter_count: int, logging_level: str):

    return f"""\
  {filter_type}_lb_proxy:
    build:
      context: ./
      dockerfile: ./src/controllers/load_balancer_proxy/Dockerfile
    container_name: {filter_type}_proxy
    image: lb_proxy:latest
    entrypoint: python3 main.py
    environment:
      - PYTHONHASHSEED=86
      - FILTER_TYPE={filter_type}
      - FILTER_COUNT={filter_count}
      - INPUT_QUEUES={input_queues}
      - LOGGING_LEVEL={logging_level}
      - KEY_TO_HASH={key_to_hash}
      - IS_PROXY={0 if key_to_hash == "0" else 1}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/load_balancer_proxy/lb_proxy.py:/main.py
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
      - ./state/{filter_type}_lb_proxy/:/state
"""


def date_filter_service_text(number: int, items_per_batch: int):
    return f"""\
  date_filter{number}:
    build:
      context: ./
      dockerfile: ./src/controllers/filters/date_filter/Dockerfile
    container_name: date_filter{number}
    image: date_filter:latest
    entrypoint: python3 main.py
    environment:
      - ITEMS_PER_BATCH={items_per_batch}
      - FILTER_NUMBER={number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/filters/date_filter/config.ini:/config.ini
      - ./src/controllers/filters/date_filter/main.py:/main.py
      - ./state/date_filter{number}/:/state
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
"""


def category_filter_service_text(number: int, items_per_batch: int):
    return f"""\
  category_filter{number}:
    build:
      context: ./
      dockerfile: ./src/controllers/filters/category_filter/Dockerfile
    container_name: category_filter{number}
    image: category_filter:latest
    entrypoint: python3 main.py
    environment:
      - ITEMS_PER_BATCH={items_per_batch}
      - FILTER_NUMBER={number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/filters/category_filter/config.ini:/config.ini
      - ./src/controllers/filters/category_filter/main.py:/main.py
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
      - ./state/category_filter{number}/:/state
"""


def title_filter_service_text(number: int, items_per_batch: int):
    return f"""\
  title_filter{number}:
    build:
      context: ./
      dockerfile: ./src/controllers/filters/title_filter/Dockerfile
    container_name: title_filter{number}
    image: title_filter:latest
    entrypoint: python3 main.py
    environment:
      - ITEMS_PER_BATCH={items_per_batch}
      - FILTER_NUMBER={number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/filters/title_filter/config.ini:/config.ini
      - ./src/controllers/filters/title_filter/main.py:/main.py
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
      - ./state/title_filter{number}/:/state
"""


def decade_counter_service_text(filter_number: int):
    return f"""\
  decade_counter{filter_number}:
    build:
      context: ./
      dockerfile: ./src/controllers/filters/decade_counter/Dockerfile
    container_name: decade_counter{filter_number}
    image: decade_counter:latest
    entrypoint: python3 main.py
    environment:
      - FILTER_NUMBER={filter_number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/filters/decade_counter/config.ini:/config.ini
      - ./src/controllers/filters/decade_counter/main.py:/main.py
      - ./state/decade_counter{filter_number}/:/state
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
"""


def sentiment_analyzer_service_text(number: int):
    return f"""\
  sentiment_analyzer{number}:
    build:
      context: ./
      dockerfile: ./src/controllers/filters/sentiment_analyzer/Dockerfile
    container_name: sentiment_analyzer{number}
    image: sentiment_analyzer:latest
    entrypoint: python3 main.py
    environment:
      - ITEMS_PER_BATCH=700
      - FILTER_NUMBER={number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/filters/sentiment_analyzer/config.ini:/config.ini
      - ./src/controllers/filters/sentiment_analyzer/main.py:/main.py
      - ./state/sentiment_analyzer{number}/:/state
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
   """


def joiner_text(number: int):
    return f"""\
  review_joiner{number}:
    build:
      context: ./
      dockerfile: ./src/controllers/review_joiner/Dockerfile
    container_name: review_joiner{number}
    image: review_joiner:latest
    entrypoint: python3 review_joiner.py
    environment:
      - FILTER_NUMBER={number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/review_joiner/config.ini:/config.ini
      - ./src/controllers/review_joiner/review_joiner.py:/review_joiner.py
      - ./state/review_joiner{number}/:/state
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py

"""


def review_counter_text(filter_number: int):
    return f"""\
  review_counter{filter_number}:
    build:
      context: ./
      dockerfile: ./src/controllers/filters/review_counter/Dockerfile
    container_name: review_counter{filter_number}
    image: review_counter:latest
    entrypoint: python3 review_counter.py
    environment:
      - FILTER_NUMBER={filter_number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/filters/review_counter/config.ini:/config.ini
      - ./src/controllers/filters/review_counter/review_counter.py:/review_counter.py
      - ./state/review_counter{filter_number}/:/state
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
"""


def sentiment_average_reducer_text(number: int):
    return f"""\
  sentiment_average_reducer{number}:
    build:
      context: ./
      dockerfile: ./src/controllers/filters/sentiment_average_reducer/Dockerfile
    container_name: sentiment_average_reducer{number}
    image: sentiment_average_reducer:latest
    entrypoint: python3 main.py
    environment:
      - FILTER_NUMBER={number}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/filters/sentiment_average_reducer/config.ini:/config.ini
      - ./src/controllers/filters/sentiment_average_reducer/main.py:/main.py
      - ./state/review_averager{number}/:/state
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
"""

def medic_service_text(medic_number: int, no_of_medics: int):
    return f"""\
  medic{medic_number}:
    build:
      context: ./
      dockerfile: ./src/controllers/medic/Dockerfile
    container_name: medic{medic_number}
    image: medic:latest
    entrypoint: python3 medic.py
    environment:
      - MEDIC_NUMBER={medic_number}
      - NUMBER_OF_MEDICS={no_of_medics}
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/medic/controllers_to_check:/controllers_to_check
      - /var/run/docker.sock:/var/run/docker.sock
      - ./src/controllers/medic/config.ini:/config.ini
      - ./src/controllers/medic/medic.py:/medic.py
"""

def lb_proxy_for_joiner(joiner_count: int):
    return f"""\
  review_joiner_lb_proxy:
    build:
      context: ./
      dockerfile: ./src/controllers/load_balancer_proxy_for_joiner/Dockerfile
    container_name: review_joiner_proxy
    image: load_balancer_proxy_for_joiner:latest
    entrypoint: python3 main.py
    environment:
      - PYTHONHASHSEED=86
      - FILTER_COUNT={joiner_count}
      - LOGGING_LEVEL=INFO
      - SIM_CRASH={CRASH_CONTROLLERS_BY_DEFAULT}
    networks:
      - tp1_testing_net
    volumes:
      - ./src/controllers/load_balancer_proxy_for_joiner/lb_proxy_for_joiner.py:/main.py
      - ./state/review_joiner_lb_proxy/:/state
      - ./src/controllers/common/healthcheck_handler.py:/src/controllers/common/healthcheck_handler.py
"""

if __name__ == "__main__":
    # Create the docker-compose.yml file
    with open("docker-compose.yml", "w") as f:
      with open("src/controllers/medic/controllers_to_check", "w") as m:
          

          # Write the header
          f.write(header_text())
          f.write("\n")

          # Medics
          for i in range(1, MEDIC_COUNT + 1):
              f.write(medic_service_text(i, MEDIC_COUNT))
              f.write("\n")

          # Boundary
          f.write(boundary_service_text())
          f.write("\n")

          # Title filters & lb_proxy
          f.write(
              proxy_service_text(
                  "title_filter","title_filter_queue","title", TITLE_FILTER_COUNT, BARRIER_LOGGING_LEVEL
              )
          )
          f.write("\n")
          m.write("title_filter_proxy\n")

          for i in range(1, TITLE_FILTER_COUNT + 1):
              f.write(title_filter_service_text(i, ITEMS_PER_BATCH))
              f.write("\n")
              m.write(f"title_filter{i}\n")

          # Date filters & barrier
          f.write(
              proxy_service_text(
                  "date_filter", "date_filter_queue", "title", DATE_FILTER_COUNT, BARRIER_LOGGING_LEVEL
              )
          )
          f.write("\n")
          m.write("date_filter_proxy\n")

          for i in range(1, DATE_FILTER_COUNT + 1):
              f.write(date_filter_service_text(i, ITEMS_PER_BATCH))
              f.write("\n")
              m.write(f"date_filter{i}\n")

          # Category filters & barrier
          f.write(
              proxy_service_text(
                  "category_filter", "category_filter_queue", "title", CATEGORY_FILTER_COUNT, BARRIER_LOGGING_LEVEL
              )
          )
          f.write("\n")
          m.write(f"category_filter_proxy\n")

          for i in range(1, CATEGORY_FILTER_COUNT + 1):
              f.write(category_filter_service_text(i, ITEMS_PER_BATCH))
              f.write("\n")
              m.write(f"category_filter{i}\n")

          # Review Counter
          f.write(
              proxy_service_text(
                  "review_counter", "review_counter_queue", "conn_id", REVIEW_COUNTER_COUNT, BARRIER_LOGGING_LEVEL
              )
          )
          f.write("\n")
          m.write(f"review_counter_proxy\n")

          for i in range(1, REVIEW_COUNTER_COUNT + 1):
              f.write(review_counter_text(i))
              f.write("\n")
              m.write(f"review_counter{i}\n")

          # Decade Counter
          f.write(
              proxy_service_text(
                  "decade_counter", "decade_counter_queue", "authors", DECADE_COUNTER_COUNT, BARRIER_LOGGING_LEVEL
              )
          )
          f.write("\n")
          m.write(f"decade_counter_proxy\n")

          for i in range(1, DECADE_COUNTER_COUNT + 1):
              f.write(decade_counter_service_text(i))
              f.write("\n")
              m.write(f"review_counter{i}\n")

          # Sentiment Analyzer
          f.write(
              proxy_service_text(
                  "sentiment_analyzer", "sentiment_analyzer_queue", "title", SENTIMENT_ANALYZER_COUNT, BARRIER_LOGGING_LEVEL
              )
          )
          f.write("\n")
          m.write(f"sentiment_analyzer_proxy\n")

          for i in range(1, SENTIMENT_ANALYZER_COUNT + 1):
              f.write(sentiment_analyzer_service_text(i))
              f.write("\n")
              m.write(f"sentiment_analyzer{i}\n")

          # Reviews Joiner
          f.write(
              lb_proxy_for_joiner(REVIEW_JOINER_COUNT)
          )
          f.write("\n")
          m.write(f"review_joiner_proxy\n")

          for i in range(1, REVIEW_JOINER_COUNT + 1):
              f.write(joiner_text(i))
              f.write("\n")
              m.write(f"review_joiner{i}\n")


          # Sentiment Averager
          f.write(
              proxy_service_text(
                  "sentiment_averager", "sentiment_averager_queue", "conn_id", SENTIMENT_AVERAGER_COUNT, BARRIER_LOGGING_LEVEL
              )
          )
          f.write("\n")
          m.write(f"sentiment_averager_proxy\n")

          for i in range(1, SENTIMENT_AVERAGER_COUNT + 1):
              f.write(sentiment_average_reducer_text(i))
              f.write("\n")
              m.write(f"sentiment_average_reducer{i}\n")

          # Write the footer
          f.write(footer_text())


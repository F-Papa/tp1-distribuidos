SHELL := /bin/bash
PWD := $(shell pwd)

network-create:
	docker network create tp1_testing_net

rabbit-up:
	docker run --rm --network tp1_testing_net --name rabbit -p 15672:15672 rabbitmq:3.13-management

filters-build:
	docker compose -f ./docker-compose.yml build

filters-up:
	docker compose -f ./docker-compose.yml up -d

filters-logs:
	docker compose -f ./docker-compose.yml logs -f

filters-down:
	docker compose -f ./docker-compose.yml down

boundary-up:
	docker run -it -e LOGGING_LEVEL=INFO -v ./boundary.py:/main.py -v ./data/books_data.csv:/books.csv -v ./data/Books_ratings.csv:/reviews.csv --rm --network tp1_testing_net boundary

boundary-build:.
	docker build -f ./Dockerfile -t boundary .


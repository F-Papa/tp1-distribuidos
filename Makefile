SHELL := /bin/bash
PWD := $(shell pwd)

BOUNDARY_ITEMS_PER_BATCH := 100
BOOKS_ENTITY := ./data/books_data.csv
REVIEWS_ENTITY := ./data/Books_rating.csv

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
	docker compose -f ./docker-compose.yml down -t 0
	sudo rm -R ./state

boundary-up:
	docker run -it -e LOGGING_LEVEL=INFO -e ITEMS_PER_BATCH=$(BOUNDARY_ITEMS_PER_BATCH) -v ./boundary.py:/main.py -v ./results:/results -v $(BOOKS_ENTITY):/books.csv -v $(REVIEWS_ENTITY):/reviews.csv --rm --network tp1_testing_net boundary

boundary-build:.
	docker build -f ./Dockerfile -t boundary .


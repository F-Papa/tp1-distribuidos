SHELL := /bin/bash
PWD := $(shell pwd)

create-network:
	docker network create tp1_testing_net

rabbit:
	docker run --rm --network tp1_testing_net --name rabbit -p 15672:15672 rabbitmq:3.13-management

build-filters:
	docker compose -f ./docker-compose.yml build

filters:
	docker compose -f ./docker-compose.yml up -d

filters-logs:
	docker compose -f ./docker-compose.yml logs -f

build-boundary:.
	docker build -f ./Dockerfile -t boundary .
	
boundary:
	docker run -it -v ./boundary.py:/main.py -v ./data/books_data.csv:/books.csv -v ./data/Books_ratings.csv:/reviews.csv --rm --network tp1_testing_net boundary


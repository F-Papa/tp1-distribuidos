# Instructions

1) Run the `rabbit.sh` script to start RabbitMQ.
> $ ./rabbit.sh
2) Start the filters and watch the logs with docker compose
> $ docker compose up -d && docker compose logs -f


# Shutting the system down

1) Kill the filters with docker compose.
> $ docker compose down -t 0
2) Shutdown RabbitMQ
> $ docker kill rabbit
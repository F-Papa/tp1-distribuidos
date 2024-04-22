# Instructions

1) Create the network if it is the first time running the system
> $ make network-create

2) Start RabbitMQ.
> $ make rabbit-up

3) Build the System
> $ make filters-build 

4) Run the System
> $ make filters-up

5) Build the boundary
> $ make boundary-build 

4) Run the Boundary
> $ make boundary-up

# Shutting the system down

1) Terminate the filters.
> $ make filters-down

2) Shutdown RabbitMQ and Boundary Containers
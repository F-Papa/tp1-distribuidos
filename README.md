# Amazon Books Analyzer

## Introduction

This system's purpouse is to analyze books and their revi

## Documentation

## 4+1 View

### Scenarios

![Scenario Diagram](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramasTp1-Usecases.drawio.png?raw=true)

The 5 scenarios in which the user may interact with the system.

### Physical
![Robustness Diagram](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramasTp1-ROBUSTEZ_FINAL.png?raw=true)

Robustness diagram containing the entities, control objects, and boundaries of the system.

![Deployment Diagram](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramasTp1-DESPLIEGUE_FINAL.png?raw=true)

Deployment diagram modeling the phyisical deployment of nodes.

### Logical

![Class Diagram](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramasTp1-Clases.png?raw=true)

Classes present in the system's design and their relationships.

### Process

![Activity Diagram Query1](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramadeActividades1-Activity_Query1_FINAL.png?raw=true)

The Input controller sends data to Date Proxy Barrier and eventually sends an End Of File message. In the meantime this data travels through different filters. In those filters that can be clustered, a barrier proxy is present, distributing the workload evenly and ensures that all its filters are done before forwarding the End Of File message.

![Sequence Diagram Query1](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramasTp1-Secuencia1.png?raw=true)

A user feeds data to the system and obtains the titles, authors, and publishers of distributed computer books published between 2000 and 2023.

![Activity Diagram Query5](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramadeActividades1-Activity_Query5_FINAL.png?raw=true)

A more complex query, invoving the reviews entity is performed, a joiner is needed to match reviews with their corresponding book data.


### Development

![Package Diagram](https://github.com/F-Papa/tp1-distribuidos/blob/main/diagrams/DiagramasTp1-Paquetes.png?raw=true) 

The packages in which the source code is separated and their dependencies.

## Instructions

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

## Shutting the system down

1) Terminate the filters.
> $ make filters-down

2) Shutdown RabbitMQ and Boundary Containers

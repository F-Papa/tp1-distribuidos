# Amazon Books Analyzer

## About the System

The system in question is a distributed system that processes large data sets corresponding to books and their reviews from Amazon, simulating the results obtained by a web scraper. Join, Filter, Count, and Reduce operations are performed on this data. The connection to the 'border' of the system is made with a client via TCP and the two sets of data are sent to it by this means. The results are delivered once they are ready through the already opened connection and are stored in the directory specified by the user.

Queries range from the top rated books from the 90s to those under the "Fiction" category whose average sentiment is among the 90th quantile.

## Instructions

### Starting the System Up

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

### Connecting to the system

1) With client directory as CWD execute:
> $ python3 client.py -b <path_to_books> --reviews <path_to_reviews> 

Optionally, the output directory can be specified as follows
> $ python3 client.py -b <path_to_books> --reviews <path_to_reviews> --results <path_to_results>


### Shutting the system down

1) Terminate the filters and reset the state.
> $ make filters-down

2) Shutdown RabbitMQ and Boundary Containers

# Further Information
Diagrams and more detailed documentation can be found at: https://docs.google.com/document/d/1YRLNbbPteX_j9Rn3jgoasGVM26Hs3afK109QtxntNCg/edit?usp=sharing

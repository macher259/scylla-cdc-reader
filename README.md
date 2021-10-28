# SCYLLA CDC Reader #
Program to read cdc table with given stream id in ScyllaDB.
Made using Rust.
## Usage ##
First, launch [ScyllaDB](https://www.scylladb.com/) locally or in a container.

I assume that Scylla runs on [172.17.0.2] on port [9042].

To launch program:
`./scylla-cdc-reader STREAM`  
where\
`STREAM` is a mandatory parameter with a [stream id](https://docs.scylladb.com/using-scylla/cdc/cdc-streams/) that we
want to be reading.

Program assumes that the base table was created by using: \

#!/bin/bash
pkill -f "server -id"
go build -o bin/server ./cmd/server

# Node 1
./bin/server -id 1 -port 50051 -peers 2=localhost:50052,3=localhost:50053 &

# Node 2 (Make sure ID is 2)
./bin/server -id 2 -port 50052 -peers 1=localhost:50051,3=localhost:50053 &

# Node 3 (Make sure ID is 3)
./bin/server -id 3 -port 50053 -peers 1=localhost:50051,2=localhost:50052 &

echo "Cluster started with 3 nodes."
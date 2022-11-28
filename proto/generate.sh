#!/usr/bin/env sh

protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    --plugin=/home/ruben/go/bin kafgo.proto

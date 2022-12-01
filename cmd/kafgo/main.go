package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/rubensseva/kafgo/pkg/kafgo"
	"github.com/rubensseva/kafgo/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	lis, err := net.Listen("tcp", "localhost:5000")
	if err != nil {
		fmt.Printf("failed to listen: %v\n", err)
		os.Exit(1)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	server := &kafgo.KafgoServer{
		Rdb: rdb,
		Chs: map[string][]chan *kafgo.Msg{},
		Mu:  sync.Mutex{},
	}
	proto.RegisterKafgoServer(grpcServer, server)

	reflection.Register(grpcServer)

	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v\n", err)
		os.Exit(1)
	}
}

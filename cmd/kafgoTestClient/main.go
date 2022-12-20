package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/rubensseva/kafgo/proto"
	"google.golang.org/grpc"
)

const (
	serverAddr = "localhost:5000"
)

func main() {
	fmt.Printf("client starting\n")

	err := publishSome("test-topic")
	if err != nil {
		fmt.Printf("publishing: %v\n", err)
	}

	fmt.Printf("client main thread done\n")
}

func publishSome(topic string) error {
	fmt.Printf("publish starting\n")
	opts := grpc.WithInsecure()
	conn, err := grpc.Dial(serverAddr, opts)
	if err != nil {
		return fmt.Errorf("error when dialing gRPC: %v", err)
	}
	defer conn.Close()
	client := proto.NewKafgoClient(conn)
	ctx := context.Background()

	var wg sync.WaitGroup
	for _i := 0; _i < 10000; _i++ {
		i := _i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, serr := client.Publish(ctx, &proto.Msg{
				Topic:   topic,
				Payload: fmt.Sprintf("payload num: %d", i),
			})
			if serr != nil {
				fmt.Printf("publishing message: %v\n", serr)
				return
			}
			fmt.Printf("published %d\n", i)
		}()
	}
	wg.Wait()
	fmt.Printf("publish done\n")
	return nil
}

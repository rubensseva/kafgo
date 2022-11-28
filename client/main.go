package main

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/rubensseva/kafgo/proto"
	"google.golang.org/grpc"
)

const (
	serverAddr = "localhost:5000"
)

func main() {
	fmt.Printf("client starting\n")
	go func() {
		err := sub("test-topic")
		if err != nil {
			fmt.Printf("sub err: %v\n", err)
		}
		fmt.Printf("sub done\n")
	}()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := publishSome("test-topic")
			if err != nil {
				fmt.Printf("publish err %v\n", err)
			}
			fmt.Printf("done publishing\n")

		}()
	}

	wg.Wait()
	fmt.Printf("client main thread done\n")
}

func sub(topic string) error {
	fmt.Printf("sub starting\n")
	opts := grpc.WithInsecure()
	conn, err := grpc.Dial(serverAddr, opts)
	if err != nil {
		return fmt.Errorf("error when dialing gRPC: %v", err)
	}
	defer conn.Close()
	client := proto.NewKafgoClient(conn)
	stream, err := client.Subscribe(context.Background(), &proto.SubscribeRequest{
		Topic: topic,
	})
	waitc := make(chan struct{})
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			close(waitc)
			return nil
		}
		if err != nil {
			fmt.Printf("Error when receiving message: %v\n", err)
			return fmt.Errorf("error when receiving message: %v", err)
		}
		fmt.Printf("Got message :%v\n", in)
	}
	fmt.Printf("sub done\n")
	return nil
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
	for i := 0; i < 100; i++ {
		_, serr := client.Publish(ctx, &proto.Msg{
			Topic: topic,
			Payload: fmt.Sprintf("payload num: %d", i),
		})
		if serr != nil {
			return fmt.Errorf("publishing message: %v", serr)
		}
	}
	fmt.Printf("publish done\n")
	return nil
}

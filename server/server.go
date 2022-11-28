package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rubensseva/kafgo/proto"
)

var (
	chs = map[string]([] chan *Msg) {}
)

func rmChan(topic string, ch chan *Msg) {
	s := chs[topic]
	for i := range s {
		if ch == s[i] {
			new := [] chan *Msg {}
			new = append(new, s[:i]...)
			new = append(new, s[i + 1:]...)
			s = new
		}
	}
}

type KafgoServer struct {
	proto.UnimplementedKafgoServer
}

func (s *KafgoServer) Subscribe(req *proto.SubscribeRequest, stream proto.Kafgo_SubscribeServer) error {
	if req.Topic == "" {
		fmt.Printf("Attempted to subscribe, but topic was empty\n")
		return fmt.Errorf("attempted to subscribe, but topic was empty")
	}

	ch := make(chan *Msg)
	chs[req.Topic] = append(chs[req.Topic], ch)
	defer rmChan(req.Topic, ch)

	fmt.Printf("Got a new subscription. All subscriptions: %v\n", chs)

	for {
		select {
		    // If the client disconnects
		    case <-stream.Context().Done():
			    fmt.Printf("Client disconnected, on topic %v\n", req.Topic)
			    return nil
			// If we get a new message on this topic
		    case m := <- ch:
				fmt.Printf("Subscription on topic <%v> got a new message: %v\n", req.Topic, m)
				if err := stream.Send(m.toProto()); err != nil {
					fmt.Printf("received error when sending msg: %v\n", err)
				}
		}
	}
}

func (s *KafgoServer) Publish(ctx context.Context, msg *proto.Msg) (*proto.PublishResponse, error) {
	if msg.Topic == "" {
		fmt.Printf("Attempted to publish, but topic was empty\n")
		return nil, fmt.Errorf("attempted to publish, but topic was empty")
	}
	received := time.Now()
	new := msgFromProto(msg, received)

	// Store message in db
	go func() {
		res := db.Create(new)
		if res.Error != nil {
			fmt.Printf("Error when inserting message %v in db: %v\n", new, res.Error)
			return
		}
		fmt.Printf("Created message: %v\n", msg)
	}()

	// Notify all listeners on the topic that a new message is published
	fmt.Printf("notifying topic %v\n", chs[msg.Topic])
	for _, ch := range chs[msg.Topic] {
		ch <- new
	}

	return &proto.PublishResponse{}, nil
}

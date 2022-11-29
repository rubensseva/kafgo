package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rubensseva/kafgo/proto"
)

type KafgoServer struct {
	proto.UnimplementedKafgoServer
}

var (
	chs = map[string]([]chan *Msg){}
)

func rmChan(topic string, ch chan *Msg) {
	s := chs[topic]
	for i := range s {
		if ch == s[i] {
			new := []chan *Msg{}
			new = append(new, s[:i]...)
			new = append(new, s[i+1:]...)
			chs[topic] = new
		}
	}
}

func handleErr(format string, a ...any) error {
	fmt.Printf(fmt.Sprintf("%s\n", format), a...)
	return fmt.Errorf(format, a...)
}

func (s *KafgoServer) Subscribe(req *proto.SubscribeRequest, stream proto.Kafgo_SubscribeServer) error {
	if req.Topic == "" {
		return handleErr("attempted to subscribe, but topic was empty\n")
	}

	ch := make(chan *Msg)
	chs[req.Topic] = append(chs[req.Topic], ch)
	defer rmChan(req.Topic, ch)

	fmt.Printf("got a new subscription. All subscriptions: %v\n", chs)

	ctx := context.Background()
	blockedFromStr, err := rdb.Get(ctx, fmt.Sprintf("%s:blocked-from", req.Topic)).Result()
	if err != nil && err != redis.Nil {
		return handleErr("getting blocked-from val from redis: %v\n", err)
	}
	if blockedFromStr != "" {
		blockedFrom, err := strconv.ParseInt(blockedFromStr, 10, 64)
		if err != nil {
			return handleErr("converting blocked-from from string to int64: %v\n", err)
		}

		res := rdb.ZRange(ctx, req.Topic, blockedFrom, -1)
		if err := res.Err(); err != nil {
			return handleErr("getting zrange: %v\n", err)
		}
		payloads, err := res.Result()
		if err != nil {
			return handleErr("scanning redis zrange result: %v\n", err)
		}
		for _, payload := range payloads {
			stream.Send(&proto.Msg{
				Topic:   req.Topic,
				Payload: payload,
			})
		}

		delRes := rdb.Del(ctx, fmt.Sprintf("%s:blocked-from", req.Topic))
		if err := delRes.Err(); err != nil {
			return handleErr("deleting blocked-from from redis: %v", err)
		}
	}

	for {
		select {
		// If the client disconnects
		case <-stream.Context().Done():
			fmt.Printf("client disconnected, on topic %v\n", req.Topic)
			return nil
			// If we get a new message on this topic
		case m := <-ch:
			if err := stream.Send(m.toProto()); err != nil {
				fmt.Printf("received error when sending msg: %v\n", err)
			}
		}
	}
}

func (s *KafgoServer) Publish(ctx context.Context, msg *proto.Msg) (*proto.PublishResponse, error) {
	if msg.Topic == "" {
		return nil, handleErr("Attempted to publish, but topic was empty\n")
	}
	received := time.Now()
	new := msgFromProto(msg, received)

	// Store message in redis
	z := &redis.Z{
		Score:  float64(new.Received),
		Member: new.Payload,
	}
	res := rdb.ZAdd(ctx, new.Topic, z)
	err := res.Err()
	if err != nil {
		return nil, handleErr(
			"inserting value into redis sorted set, topic: %v, time: %v, payload: %v, err: %v\n",
			new.Topic,
			new.Received,
			new.Payload,
			err)
	}

	// If three are no subscribers, we need to store the time from when we started to
	// receive messages that are not being sent. When a subscribers connect, we can then
	// send all messages from that time.
	if len(chs[new.Topic]) == 0 {
		val, err := rdb.Get(ctx, fmt.Sprintf("%s:blocked-from", new.Topic)).Result()
		if err != nil {
			return nil, handleErr("getting blocked-from val from redis: %v\n", err)
		}
		if val == "" {
			res := rdb.Set(ctx, fmt.Sprintf("%s:blocked-from", new.Topic), new.Received, 0)
			err := res.Err()
			if err != nil {
				return nil, handleErr("setting block-from val: %v", err)
			}
		}
	}

	// Notify all listeners on the topic that a new message is published
	for _, ch := range chs[new.Topic] {
		ch <- new
	}

	return &proto.PublishResponse{}, nil
}

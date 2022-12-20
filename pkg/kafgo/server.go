package kafgo

import (
	"context"
	"fmt"

	"github.com/rubensseva/kafgo/proto"
)

type KafgoGRPCServer struct {
	proto.UnimplementedKafgoServer
	KServer KafgoServer
}

func (s *KafgoGRPCServer) Subscribe(req *proto.SubscribeRequest, stream proto.Kafgo_SubscribeServer) error {
	if req.Topic == "" {
		return handleErr("attempted to subscribe, but topic was empty\n")
	}

	return s.KServer.sub(req.Topic, req.SubGroup, stream)
}

func (s *KafgoGRPCServer) Publish(ctx context.Context, msg *proto.Msg) (*proto.PublishResponse, error) {
	if msg.Topic == "" {
		return nil, handleErr("Attempted to publish, but topic was empty\n")
	}

	err := s.KServer.pub(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("publishing message: %v\n", err)
	}
	return &proto.PublishResponse{}, nil
}

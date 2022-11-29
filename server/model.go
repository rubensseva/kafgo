package main

import (
	"gorm.io/gorm"
	"time"

	"github.com/rubensseva/kafgo/proto"
)

type Msg struct {
	gorm.Model
	Received int64 // unix nano
	Topic    string
	Payload  string
}

func (m *Msg) toProto() *proto.Msg {
	return &proto.Msg{
		Topic:   m.Topic,
		Payload: m.Payload,
	}
}

func msgFromProto(p *proto.Msg, received time.Time) *Msg {
	return &Msg{
		Received: received.UnixNano(),
		Topic:    p.Topic,
		Payload:  p.Payload,
	}
}

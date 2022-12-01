package kafgo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rubensseva/kafgo/proto"
)

// Simple redis mock that returns success, or an error indicating the
// index was not found
type RedisClientMock struct{}

func (r RedisClientMock) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := &redis.StringCmd{}
	cmd.SetErr(redis.Nil)
	return cmd
}
func (r RedisClientMock) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := &redis.IntCmd{}
	cmd.SetErr(redis.Nil)
	return cmd
}
func (r RedisClientMock) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return &redis.StatusCmd{}
}
func (r RedisClientMock) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	return &redis.IntCmd{}
}
func (r RedisClientMock) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	cmd := &redis.StringSliceCmd{}
	cmd.SetErr(redis.Nil)
	return cmd
}

func TestKafgoServer_pub(t *testing.T) {
	type fields struct {
		Rdb RedisClient
		Chs map[string]([]chan *Msg)
		Mu  *sync.Mutex
	}
	type args struct {
		ctx context.Context
		msg *proto.Msg
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				Rdb: RedisClientMock{},
				Chs: map[string][]chan *Msg{},
				Mu:  &sync.Mutex{},
			},
			args: args{
				ctx: nil,
				msg: &proto.Msg{
					Topic:   "house atreides",
					Payload: "fear is the mind killer",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &KafgoServer{
				Rdb: tt.fields.Rdb,
				Chs: tt.fields.Chs,
				Mu:  tt.fields.Mu,
			}
			if err := s.pub(tt.args.ctx, tt.args.msg); (err != nil) != tt.wantErr {
				t.Errorf("KafgoServer.pub() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

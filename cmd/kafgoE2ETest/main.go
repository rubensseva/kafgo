package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/rubensseva/kafgo/proto"
	"google.golang.org/grpc"
)

type msgRecord struct {
	id uuid.UUID
	num int
}

const (
	serverAddr = "localhost:5000"
)

func TODO(numRcvs []int, have []uuid.UUID) {
	missing := []uuid.UUID{}
	tooMany := []msgRecord{}

	for i, numRcv := range numRcvs {
		if numRcv == 0 {
			missing = append(missing, [i])
		}
		if numRcv > 1 {
			tooMany = append(tooMany, msgRecord{numRcvs[i], numRcv})
		}
	}
}

func main() {
	fmt.Printf("client starting\n")

	err := onePubOneSub("test-topic", "test-sub-group")
	if err != nil {
		fmt.Printf("publishing: %v\n", err)
	}

	fmt.Printf("client main thread done\n")
}

func onePubOneSub(topic string, subGroup string) error {
	fmt.Printf("publish starting\n")
	opts := grpc.WithInsecure()
	conn, err := grpc.Dial(serverAddr, opts)
	if err != nil {
		return fmt.Errorf("error when dialing gRPC: %v", err)
	}
	defer conn.Close()
	client := proto.NewKafgoClient(conn)
	ctx := context.Background()

	numMsgs := 100
	uuids := []uuid.UUID{}
	for i := 0; i < numMsgs; i++ {
		uuids = append(uuids, uuid.New())
	}

	// id := uuid.New()

	for _, id := range uuids {
		_, serr := client.Publish(ctx, &proto.Msg{
			Topic:   topic,
			Payload: id.String(),
		})
		if serr != nil {
			return fmt.Errorf("publishing message: %v\n", serr)
		}
	}

	subCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second * 5))
	defer cancel()

	stream, err := client.Subscribe(subCtx, &proto.SubscribeRequest{Topic: topic, SubGroup: subGroup})
	if err != nil {
		return fmt.Errorf("subscribing: %v", err)
	}

	receivedMsg := make([]int, len(uuids))
	for {
		feature, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receiving from gRPC stream: %v", err)
		}
		for i, id := range uuids {
			if feature.Payload == id.String() {
				log.Printf("received msg %v", feature.Payload)
				receivedMsg[i] += 1
				break
			}
		}
	}





	missing := []uuid.UUID{}
	tooMany := []msgRecord{}

	for i, numRcv := range receivedMsg {
		if numRcv == 0 {
			missing = append(missing, uuids[i])
		}
		if numRcv > 1 {
			tooMany = append(tooMany, msgRecord{uuids[i], numRcv})
		}
	}

	failedTest := false
	if len(missing) != 0 {
		failedTest = true
		log.Printf("missed response for these uuids: %v", missing)
	}

	if len(tooMany) != 0 {
		failedTest = true
		log.Printf("got too many responses for these uuids: %v", tooMany)
	}

	if failedTest {
		log.Println("test failed")
	} else {
		log.Println("test passed!")
	}

	return nil
}

func onePubMultiSub(topic string) error {
	return fmt.Errorf("not implemented yet")
}

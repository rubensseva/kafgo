# KafGo
A simple, experimental message queue. 
Implemented with Go, Redis and gRPC. 

## How to run

Start a Redis instance
``` sh
$ docker run -p 6379:6379 --name some-redis -d redis
```

Run the server
``` sh
$ cd cmd/kafgo
$ go run .
```

Publish some messages
``` sh
$ cd cmd/kafgoTestClient
$ go run .
```

Subscribe to a topic (requires `grpcurl`)
``` sh
$ grpcurl -d '{"topic": "test-topic"}' -plaintext localhost:5000 Kafgo.Subscribe
```

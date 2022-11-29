# KafGo
A simple, experimental message queue. 
Implemented with Go, Redis and gRPC. 

## How to run

``` sh
$ docker run -p 6379:6379 --name some-redis -d redis
```

Run the server
``` sh
$ cd server
$ go run .
```

Publish some messages
``` sh
$ cd client
$ go run .
```

Subscribe to topic (requires `grpcurl`)
``` sh
$ grpcurl -d '{"topic": "test-topic"}' -plaintext localhost:5000 Kafgo.Subscribe
```

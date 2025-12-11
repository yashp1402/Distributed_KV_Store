package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/yashp1402/distributed-kv-store/proto"
	"google.golang.org/grpc"
)

func main() {
	// 1. Parse Command Line Arguments
	serverAddr := flag.String("server", "localhost:50051", "Server address (host:port)")
	op := flag.String("op", "get", "Operation: get, put, or delete")
	key := flag.String("key", "", "Key to access")
	val := flag.String("val", "", "Value to set (for put)")
	flag.Parse()

	if *key == "" {
		log.Fatal("Error: Key is required")
	}

	// 2. Connect to the Server
	// We use Insecure because setting up TLS certificates for localhost is overkill for this stage
	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVStoreClient(conn)

	// 3. Execute the Operation
	// Every RPC made by this client will share this context; if the server
	// does not respond within 5 seconds we cancel the request and return
	// an error instead of hanging forever.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 4. Dispatch based on the requested operation.
	switch *op {
	case "put":
		if *val == "" {
			log.Fatal("Error: Value is required for put")
		}
		// Send Put request
		// We use a dummy client_id/request_id for now.
		// In a real system, these guarantee idempotency.
		resp, err := client.Put(ctx, &pb.PutArgs{
			Key:       *key,
			Value:     *val,
			ClientId:  "cli-user",
			RequestId: time.Now().UnixNano(),
		})
		if err != nil {
			log.Fatalf("Put failed: %v", err)
		}
		if resp.Success {
			fmt.Printf("Success: Set [%s] = '%s'\n", *key, *val)
		} else {
			// In a multi-node deployment, a false Success might mean the
			// node refused the write (e.g. it is not the leader). In that
			// case the client could retry against another node.
			fmt.Printf("Failed: Server rejected the request (maybe try another node?)\n")
		}
	case "get":
		// Get reads the value associated with the given key.
		// The server’s consistency semantics (e.g. linearizable vs stale read)
		// depend on whether it routes reads through a leader or a quorum.
		resp, err := client.Get(ctx, &pb.GetArgs{Key: *key})
		if err != nil {
			log.Fatalf("Get failed: %v", err)
		}
		if resp.Found {
			fmt.Printf("Value: %s\n", resp.Value)
		} else {
			fmt.Printf("Key [%s] not found\n", *key)
		}
	case "delete":
		// Delete removes the given key from the KV store.
		// The implementation on the server side is responsible for turning
		// this into a replicated log entry via Paxos (just like Put).
		resp, err := client.Delete(ctx, &pb.DeleteArgs{Key: *key})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}
		if resp.Success {
			fmt.Printf("Deleted key [%s]\n", *key)
		} else {
			fmt.Printf("Delete failed\n")
		}
	default:
		log.Fatalf("Unknown operation: %s", *op)
	}
}

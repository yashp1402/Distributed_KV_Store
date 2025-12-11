package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/yashp1402/distributed-kv-store/pkg/paxos"
)

func main() {
	// The peers list lets each node know how to contact the others for
	// Paxos RPCs (Prepare/Accept/Decided/CatchUp).
	id := flag.Int64("id", 1, "Node ID")
	port := flag.String("port", "50051", "Port to listen on")
	peerFlag := flag.String("peers", "", "Comma-separated peer list (id=host:port)")
	flag.Parse()

	// Build a mapping from peer ID -> address ("host:port").
	peerMap := make(map[int64]string)
	if *peerFlag != "" {
		peerList := strings.Split(*peerFlag, ",")
		for _, p := range peerList {
			parts := strings.Split(p, "=")
			if len(parts) == 2 {
				var peerID int64
				fmt.Sscanf(parts[0], "%d", &peerID)
				peerMap[peerID] = parts[1]
			}
		}
	}
	// Initialize a Paxos-backed KV node.
	// NewPaxosNode is responsible for:
	//   - setting up the internal Paxos state (promised/accepted values),
	//   - initializing the replicated log and in-memory key/value store,
	//   - preparing gRPC client connections to peers using peerMap,
	//   - wiring the KVStore service implementation to the server.
	node := paxos.NewPaxosNode(*id, *port, peerMap)
	// Start the node’s gRPC server and any background goroutines
	// Start blocks until the server stops or returns an error.
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

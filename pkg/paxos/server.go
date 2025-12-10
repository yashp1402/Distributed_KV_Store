package paxos

import (
	"context"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/yashp1402/distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Represents an in-memory entry in replicated log.
// Each entry represents 1 paxos instance/1 log slot.
type LogEntry struct {
	// String encoded op
	Command string
	// Proposal number of lastAccepted value
	AcceptedProposal int64
}

// On disk representation of Paxos state, written to node's
// Write Ahead Log
type DiskEntry struct {
	// "PROMISE", "ACCEPT", or "COMMIT"
	Type string
	// Log index
	Seq int64
	// Highest propoal # accepted ofr this Seq
	MinProposal int64
	// Accepted proposal #
	AcceptedProposal int64
	Command          string
}

// Single node in distributed KV store. Implements gRPC service
// and acts as Paxos proposer, acceptor and learner.
// Responsibilities include:
// - Exposing Get/Put/Delete to clients.
// - Run Paxos to replicate a log of commands.
// - Apply committed commands to an in memory KV store
// - Persist Paxos/WAL state for crash recovery.
// - Helps to sync up via CatchUp
type PaxosNode struct {
	pb.UnimplementedKVStoreServer

	// Node's identity
	id      int64
	port    string
	peerMap map[int64]string
	peers   map[int64]pb.KVStoreClient

	// Local state protected by mu
	mu          sync.Mutex
	store       map[string]string
	minProposal int64
	log         map[int64]*LogEntry
	commitIndex int64
	lastApplied int64

	// WAL for durability
	walFile    *os.File
	walEncoder *gob.Encoder
}

// replayWAL: Replays all WAL entries from disk and rebuilds minProposal,
// log and commitIndex. Called at node construction time, so restarted node can rejoin the cluster with previous state.
func (n *PaxosNode) replayWAL() {
	f, err := os.Open(n.walFile.Name())
	// If no existing WAL, then start with empty state.
	if err != nil {
		return
	}
	defer f.Close()

	decoder := gob.NewDecoder(f)

	for {
		var entry DiskEntry
		// EOF or Decode error => stop replay
		if err := decoder.Decode(&entry); err != nil {
			break
		}

		// Restore the highest proposal number we promised.
		if entry.MinProposal > n.minProposal {
			n.minProposal = entry.MinProposal
		}

		// Rebuilding log for ACCEPT/COMMIT entries.
		if entry.Type == "ACCEPT" || entry.Type == "COMMIT" {
			n.log[entry.Seq] = &LogEntry{
				Command:          entry.Command,
				AcceptedProposal: entry.AcceptedProposal,
			}
		}

		// Update commitIndex for committed slots.
		if entry.Type == "COMMIT" {
			if entry.Seq > n.commitIndex {
				n.commitIndex = entry.Seq
			}
		}
		fmt.Printf("[%d] Recovered from Disk. Log Size: %d. MinProposal: %d\n", n.id, len(n.log), n.minProposal)
	}
}

// NewPaxosNode: Creates a new PaxosNode with given identity, listening port
// and mapping of peerID to addresses. Opens/Creates the WAL file,
// initializes in-memory state and replays WA for resuming previous Paxos state it had before crashing.
func NewPaxosNode(id int64, port string, peerMap map[int64]string) *PaxosNode {
	filename := fmt.Sprintf("node-%d.wal", id)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	n := &PaxosNode{
		id:          id,
		port:        port,
		peerMap:     peerMap,
		peers:       make(map[int64]pb.KVStoreClient),
		store:       make(map[string]string),
		log:         make(map[int64]*LogEntry),
		minProposal: 0,
		commitIndex: 0,
		lastApplied: 0,
		walFile:     f,
		walEncoder:  gob.NewEncoder(f),
	}
	n.replayWAL()
	return n
}

// Start: Launches the gRPC server for this node and starts background
// goRoutines for peer connections, log application and sync.
func (n *PaxosNode) Start() error {
	lis, err := net.Listen("tcp", ":"+n.port)
	if err != nil {
		return fmt.Errorf("falied to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, n)
	reflection.Register(grpcServer)

	// Dials outbound gRPC connections to other nodes.
	go n.connectToPeers()

	// Applies committed log entries to KV store.
	go n.RunApplyLoop()

	// Periodically asks peers for missing committed entries.
	go n.RunSyncLoop()

	fmt.Printf("Node %d listening on port %s\n", n.id, n.port)
	return grpcServer.Serve(lis)
}

// connectToPeers: Dials each peer and creates a KVStoreClient so this node can send Paxos and CatchUp RPCs to others.
func (n *PaxosNode) connectToPeers() {
	time.Sleep(2 * time.Second)
	fmt.Printf("[%d] Connecting to %d peers...\n", n.id, len(n.peerMap))
	for id, addr := range n.peerMap {
		// Prevent self connection
		if id == n.id {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("Failed to connect to peer %d: %v\n", id, err)
		}
		n.peers[id] = pb.NewKVStoreClient(conn)
		fmt.Printf("Node %d connected to peer %d at %s\n", n.id, id, addr)
	}
}

// Prepare: Implements Phase 1 of Paxos. If incoming proposal #
// is at least as large as seen till now, we do not accept lower
// numbers and optionally return a value already accepted for
// this slot.
func (n *PaxosNode) Prepare(ctx context.Context, args *pb.PrepareArgs) (*pb.PrepareReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.ProposalNumber >= n.minProposal {
		n.minProposal = args.ProposalNumber
		n.persist(DiskEntry{
			Type:        "PROMISE",
			MinProposal: n.minProposal,
		})
		var alreadyAcceptedVal string
		if entry, exists := n.log[args.Seq]; exists && entry.AcceptedProposal > 0 {
			alreadyAcceptedVal = entry.Command
		}
		fmt.Printf("[%d] Promised Proposal %d for Slot %d\n", n.id, args.ProposalNumber, args.Seq)
		return &pb.PrepareReply{
			Success:            true,
			HighestProposal:    n.minProposal,
			ValAlreadyAccepted: alreadyAcceptedVal,
		}, nil
	}
	fmt.Printf("[%d] Rejected Proposal %d (Current min: %d)\n", n.id, args.ProposalNumber, n.minProposal)
	return &pb.PrepareReply{
		Success:         false,
		HighestProposal: n.minProposal,
	}, nil
}

// Accept: Implements phase 2 of Paxos. If the incoming proposal number is at least as large as any we have promised, we ACCEPT the proposal for this slot and record it in log and WAL.
func (n *PaxosNode) Accept(ctx context.Context, args *pb.AcceptArgs) (*pb.AcceptReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.ProposalNumber >= n.minProposal {
		n.minProposal = args.ProposalNumber
		n.log[args.Seq] = &LogEntry{
			Command:          args.Value,
			AcceptedProposal: args.ProposalNumber,
		}
		n.persist(DiskEntry{
			Type:             "ACCEPT",
			Seq:              args.Seq,
			MinProposal:      n.minProposal,
			AcceptedProposal: args.ProposalNumber,
			Command:          args.Value,
		})
		fmt.Printf("[%d] Accepted Value '%s' for Slot %d\n", n.id, args.Value, args.Seq)
		return &pb.AcceptReply{
			Success:         true,
			HighestProposal: n.minProposal,
		}, nil
	}
	return &pb.AcceptReply{
		Success:         false,
		HighestProposal: n.minProposal,
	}, nil
}

// propose: It tries to get a specific command to be chosen and
// get it committed in the Paxos log.
// It keeps retrying with higher proposal # and exponential
// backoff until:
// - Paxos chooses this exact command or
// - caller cancels by exitting.
func (n *PaxosNode) propose(command string) bool {
	attemptDelay := 10 * time.Millisecond
	for {
		// Picking a slot and proposal number
		n.mu.Lock()
		seq := n.commitIndex + 1
		for _, exists := n.log[seq]; exists; _, exists = n.log[seq] {
			seq++
		}
		n.minProposal++
		myProposalNum := n.minProposal
		n.mu.Unlock()

		fmt.Printf("[%d] Attempting to propose '%s' for Slot %d with N=%d\n", n.id, command, seq, myProposalNum)

		chosenVal, success := n.runPaxos(seq, myProposalNum, command)
		if success && chosenVal == command {
			return true
		}

		// Retry with backoff if we lost the race or failed to reach a quorum.
		time.Sleep(10 * time.Millisecond)
		if attemptDelay < 1*time.Second {
			attemptDelay *= 2
		}
	}
}

// It executes a full Paxos round for a single log slot:
// Phase 1 (Prepare) and Phase 2 (Accept)
// and returns value which ended up being chosen and whether
// the round succeeded in reaching a quorum.
func (n *PaxosNode) runPaxos(seq int64, proposalNum int64, value string) (string, bool) {
	quorum := (len(n.peerMap) / 2) + 1

	// Phase 1: send Prepare to all peers
	type prepareResult struct {
		peerID int64
		reply  *pb.PrepareReply
		err    error
	}
	prepareCh := make(chan prepareResult, len(n.peers))

	for pid, client := range n.peers {
		go func(id int64, c pb.KVStoreClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			r, err := c.Prepare(ctx, &pb.PrepareArgs{
				ProposalNumber: proposalNum,
				Seq:            seq,
			})
			prepareCh <- prepareResult{id, r, err}
		}(pid, client)
	}

	// Count our own promise plus responses from peers.
	promises := 1
	highestPromisedVal := ""
	for i := 0; i < len(n.peers); i++ {
		res := <-prepareCh
		if res.err == nil && res.reply.Success {
			promises++
			if res.reply.ValAlreadyAccepted != "" {
				highestPromisedVal = res.reply.ValAlreadyAccepted
			}
		} else if res.err != nil {
			fmt.Printf("[%d] Prepare failed for peer %d: %v\n", n.id, res.peerID, res.err)
		}
	}

	if promises < quorum {
		fmt.Printf("[%d] Phase 1 failed. Got %d/%d promises\n", n.id, promises, quorum)
		return "", false
	}

	// Adopt an already-accepted value if necessary to preserve safety.
	valueToPropose := value
	if highestPromisedVal != "" {
		// fmt.Printf("[%d] Adopting value '%s' from previous leader\n", n.id, highestPromisedVal)
		valueToPropose = highestPromisedVal
	}

	// Phase 2: send Accept to all peers
	type acceptResult struct {
		peerID int64
		reply  *pb.AcceptReply
		err    error
	}
	acceptCh := make(chan acceptResult, len(n.peers))

	for pid, client := range n.peers {
		go func(id int64, c pb.KVStoreClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			r, err := c.Accept(ctx, &pb.AcceptArgs{
				ProposalNumber: proposalNum,
				Seq:            seq,
				Value:          valueToPropose,
			})
			acceptCh <- acceptResult{id, r, err}
		}(pid, client)
	}

	// Count ourselves
	accepts := 1
	for i := 0; i < len(n.peers); i++ {
		res := <-acceptCh
		if res.err == nil && res.reply.Success {
			accepts++
		}
	}
	if accepts < quorum {
		return "", false
	}

	// If we reach a quorum of accepts, we consider the value
	// chosen and mark it committed locally, then broadcast a
	// Decided notification.
	n.mu.Lock()
	n.log[seq] = &LogEntry{Command: valueToPropose, AcceptedProposal: proposalNum}
	if seq == n.commitIndex+1 {
		n.commitIndex = seq
	}
	n.mu.Unlock()

	go n.broadcastDecision(seq, valueToPropose)
	return valueToPropose, true
}

// broadcastDecision: It sends a Decided notification to all peers
// so that they can learn that (seq, val) has been chosen.
func (n *PaxosNode) broadcastDecision(seq int64, val string) {
	for _, client := range n.peers {
		go func(c pb.KVStoreClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			c.Decided(ctx, &pb.DecidedArgs{Seq: seq, Value: val})
		}(client)
	}
}

// Put: This is the client facing RPC to set a key's value. It
// wraps the operation in a command string and runs Paxos to get
// the command chosen and replcated.
func (n *PaxosNode) Put(ctx context.Context, args *pb.PutArgs) (*pb.PutReply, error) {
	command := fmt.Sprintf("PUT:%s:%s", args.Key, args.Value)
	success := n.propose(command)
	if !success {
		return &pb.PutReply{Success: false}, nil
	}
	return &pb.PutReply{Success: true}, nil
}

// Decided: This is called by other nodes once they know a
// particular (seq, value) has been chosen by Paxos. We store the
// command and mark the slot as committed in WAL, and update the
// commitIndex.
func (n *PaxosNode) Decided(ctx context.Context, args *pb.DecidedArgs) (*pb.DecidedReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("[%d] Learned that Slot %d is '%s'\n", n.id, args.Seq, args.Value)
	n.log[args.Seq] = &LogEntry{
		Command:          args.Value,
		AcceptedProposal: 0,
	}
	n.persist(DiskEntry{
		Type:    "COMMIT",
		Seq:     args.Seq,
		Command: args.Value,
	})
	if args.Seq > n.commitIndex {
		n.commitIndex = args.Seq
	}

	return &pb.DecidedReply{Success: true}, nil
}

// RunApplyLoop: This continuously applies committed log entries
// to the in memory KV store in order. This is what turns the log
// of commands into actual state that Get can read.
func (n *PaxosNode) RunApplyLoop() {
	for {
		time.Sleep(10 * time.Millisecond)
		n.mu.Lock()
		for n.lastApplied < n.commitIndex {
			seqToApply := n.lastApplied + 1
			entry, exists := n.log[seqToApply]
			// We know it's committed but don't have the value yet.
			// Wait for it to arrive via Decided/CatchUp.
			if !exists {
				break
			}
			n.applyCommand(entry.Command)
			n.lastApplied++
			fmt.Printf("[%d] Applied Slot %d: %s\n", n.id, seqToApply, entry.Command)
		}
		n.mu.Unlock()
	}
}

// applyCommand: This interprets a string encoded command and
// mutates the in-memory KVstore accordingly.
func (n *PaxosNode) applyCommand(cmd string) {
	parts := strings.SplitN(cmd, ":", 3)
	if len(parts) < 2 {
		return
	}
	op := parts[0]
	key := parts[1]
	if op == "PUT" && len(parts) == 3 {
		val := parts[2]
		n.store[key] = val
	} else if op == "DELETE" {
		delete(n.store, key)
	}
}

// Get: It returns the current value for a key from applied state.
// Consistency depends on how up to date this node is.
func (n *PaxosNode) Get(ctx context.Context, args *pb.GetArgs) (*pb.GetReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	val, found := n.store[args.Key]
	return &pb.GetReply{
		Value: val,
		Found: found,
	}, nil
}

// persist: It appends a DiskEntry to the node's WAL and syncs it.
// Used whenever we update minProposal, accept or commit value.
func (n *PaxosNode) persist(entry DiskEntry) {
	if err := n.walEncoder.Encode(entry); err != nil {
		fmt.Printf("Error writing to WAL: %v\n", err)
	}
	n.walFile.Sync()
}

// CatchUp: It returns any committed log entries which this
// particular node has with Seq > caller's LocalCommitIndex.
// It is called by lagging replicas to fill gaps in their logs.
func (n *PaxosNode) CatchUp(ctx context.Context, args *pb.CatchUpArgs) (*pb.CatchUpReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	missing := make(map[int64]string)
	for seq, entry := range n.log {
		if seq > args.LocalCommitIndex {
			missing[seq] = entry.Command
		}
	}
	return &pb.CatchUpReply{
		MissingEntries: missing,
	}, nil
}

// RunSyncLoop: It periodically asks each peer if they have
// committed entries beyond the current commitIndex. If yes, then
//
//	we pull the entries via CatchUp and mark them committed locally.
func (n *PaxosNode) RunSyncLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		n.mu.Lock()
		myIdx := n.commitIndex
		n.mu.Unlock()
		for _, client := range n.peers {
			go func(c pb.KVStoreClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				resp, err := c.CatchUp(ctx, &pb.CatchUpArgs{LocalCommitIndex: myIdx})
				if err != nil {
					return
				}
				if len(resp.MissingEntries) > 0 {
					n.mu.Lock()
					for seq, cmd := range resp.MissingEntries {
						if _, exists := n.log[seq]; !exists {
							fmt.Printf("[%d] Catch-up: Learned Slot %d: %s\n", n.id, seq, cmd)
							n.log[seq] = &LogEntry{Command: cmd, AcceptedProposal: 0}
							n.persist(DiskEntry{
								Type:    "COMMIT",
								Seq:     seq,
								Command: cmd,
							})
							if seq > n.commitIndex {
								n.commitIndex = seq
							}
						}
					}
					n.mu.Unlock()
				}
			}(client)
		}
	}
}

// Delete: Client Facing RPC to delete a key. It encodes the op as
// a command and runs Paxos to replicate it.
func (n *PaxosNode) Delete(ctx context.Context, args *pb.DeleteArgs) (*pb.DeleteReply, error) {
	command := fmt.Sprintf("DELETE:%s", args.Key)
	success := n.propose(command)
	if !success {
		return &pb.DeleteReply{Success: false}, nil
	}
	return &pb.DeleteReply{Success: true}, nil
}

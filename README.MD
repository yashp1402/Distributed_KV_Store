# Distributed Key-Value Store (Go + Multi-Paxos)

A fault-tolerant, distributed key-value store built in Go. It ensures strong consistency (Linearizability) across a cluster of nodes using the **Multi-Paxos** consensus algorithm.

The system is designed to survive node failures, network partitions, and restarts without losing data.

## Key Features

* **Consensus Engine:** Implements **Multi-Paxos** for log replication, ensuring all nodes agree on the order of operations.
* **Strong Consistency:** Guarantees that once a write is committed, subsequent reads will return the latest value.
* **Fault Tolerance:** The cluster continues to function as long as a majority (Quorum) of nodes are available (e.g., 2 out of 3).
* **Persistence (WAL):** Uses a **Write-Ahead Log** to persist state to disk, allowing nodes to recover their history after a crash.
* **Catch-Up Mechanism:** Nodes that disconnect and rejoin automatically sync missing log entries from peers.
* **gRPC Communication:** Uses Protocol Buffers for efficient, strictly-typed inter-node communication.

## Tech Stack

* **Language:** Go (Golang)
* **RPC Framework:** gRPC & Protocol Buffers
* **Storage:** In-memory Map + Disk-based WAL (Gob encoding)
* **Concurrency:** Goroutines, Channels, and Mutexes for thread-safe state management.

## Architecture

The system consists of 3 distinct layers:
1.  **RPC Layer:** Handles client requests (`Get`, `Put`, `Delete`) and inter-node consensus messages (`Prepare`, `Accept`, `Decided`).
2.  **Consensus Layer:** Managing the distributed log. It ensures that even if leaders fail, the log remains consistent.
3.  **State Machine:** A background worker applies committed log entries to the local Key-Value map in strict order.

## How to Run - Local

### Prerequisites
* Go 1.21+
* Protobuf Compiler (`protoc`)

### 1. Build the Project
```bash
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

### 2. Start the Cluster
Use the provided script to launch 3 nodes locally:
```bash
sh run_cluster.sh
```

### 3. Interact via Client
Open a new terminal to send commands.

#### Write Data (to Node 1):
```Bash
./bin/client -server localhost:50051 -op put -key user_id -val "12345"
```

#### Read Data (from Node 3 - Verifying Replication):

```Bash
./bin/client -server localhost:50053 -op get -key user_id
```

#### Delete Data
```bash
./bin/client -server localhost:50051 -op delete -key user_id
```
### Testing Fault Tolerance
Start the cluster.

* Kill Node 3 (Simulate a crash).
* Write data to Node 1 (put -key status -val active).
* Restart Node 3.
* Read from Node 3 (get -key status).

Result: Node 3 automatically catches up and returns "active".

## AWS Deployment Guide

This guide details how to deploy the Distributed Key-Value Store on AWS EC2 using the Free Tier. This setup mimics a real-world production environment with distinct nodes communicating over a private network.

> **⚠️ COST WARNING:** The AWS Free Tier covers **750 hours/month** of `t2.micro` usage. Running 3 instances simultaneously burns through this allowance 3x faster.
> * **Do not leave instances running overnight.**
> * **TERMINATE** (don't just stop) all instances immediately after finishing your tests.

---

### 1. Infrastructure Setup

#### Step A: Launch Instances
1.  Log in to the **AWS Console** and navigate to **EC2**.
2.  Click **Launch Instances**.
3.  **Name:** `Paxos-Node` (Set "Number of instances" to **3** in the summary panel).
4.  **OS Image:** Ubuntu Server 24.04 LTS (Free Tier Eligible).
5.  **Instance Type:** `t2.micro` or `t3.micro`.
6.  **Key Pair:** Create a new key pair (e.g., `paxos-key`), download the `.pem` file, and store it safely.

#### Step B: Network Security (Critical)
1.  Under **Network settings**, click **Edit**.
2.  Create a new **Security Group** named `paxos-sg`.
3.  Add the following **Inbound Rules**:

| Type | Port Range | Source | Description |
| :--- | :--- | :--- | :--- |
| **SSH** | 22 | `0.0.0.0/0` (Anywhere) | For you to log in |
| **Custom TCP** | 50051 | `0.0.0.0/0` (Anywhere) | For gRPC traffic |

#### Step C: Gather IP Addresses
Once running, go to the EC2 Dashboard and note down the IPs for all 3 nodes.

| Node | Public IP (For SSH/Client) | Private IP (For Paxos Peer Communication) |
| :--- | :--- | :--- |
| **Node 1** | `54.x.x.x` | `172.31.x.1` |
| **Node 2** | `54.y.y.y` | `172.31.x.2` |
| **Node 3** | `54.z.z.z` | `172.31.x.3` |

---

### 2. Server Configuration (Run on ALL 3 Nodes)

SSH into each node using separate terminal tabs.

```bash
# Permission the key (run locally once)
chmod 400 paxos-key.pem

# Connect (Repeat for Node 1, 2, and 3)
ssh -i "paxos-key.pem" ubuntu@<PUBLIC_IP>
```
---
### 3. Start the Cluster
You must start the servers using their Private IPs so they can talk to each other internally within the AWS VPC.

On Node 1:
```bash
./bin/server -id 1 -port 50051 -peers 2=<NODE_2_PRIVATE_IP>:50051,3=<NODE_3_PRIVATE_IP>:50051
```

On Node 2:
```bash
./bin/server -id 2 -port 50051 -peers 1=<NODE_1_PRIVATE_IP>:50051,3=<NODE_3_PRIVATE_IP>:50051
```

On Node 3:
```bash
./bin/server -id 3 -port 50051 -peers 1=<NODE_1_PRIVATE_IP>:50051,2=<NODE_2_PRIVATE_IP>:50051
```
> Note: You should see logs indicating "Connected to Peer X" on all terminals.

---

### 4. Run the Client (Locally)
Do not run the client on the servers. Run it from your local machine to simulate a real remote user.

Write Data (Connect to Node 1):
```bash
./bin/client -server <NODE_1_PUBLIC_IP>:50051 -op put -key cloud -val "AWS_Works"
```

Read Data (Connect to Node 3 - Verification):
```bash
./bin/client -server <NODE_3_PUBLIC_IP>:50051 -op get -key cloud
```

Expected Output:
```Plaintext
Value: AWS_Works
```
---

### 5. Cleanup

To avoid unexpected charges:

* Go to AWS Console > EC2 > Instances.
* Select all 3 instances.
* Click Instance State -> Terminate.

---
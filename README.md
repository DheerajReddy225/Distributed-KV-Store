# Distributed Key-Value Store

A fault-tolerant distributed KV store built with Java 17, Spring Boot, and Docker.  
Every concept from the resume bullet is implemented and explained inline.

---

## Architecture Overview

```
Client
  │
  ▼
┌─────────────┐    consistent    ┌─────────────┐    replication   ┌─────────────┐
│   node-1    │◄── hash ring ───►│   node-2    │◄── fan-out ─────►│   node-3    │
│  :8081      │                  │  :8082      │                  │  :8083      │
│             │                  │             │                  │             │
│ WAL + mem   │                  │ WAL + mem   │                  │ WAL + mem   │
└─────────────┘                  └─────────────┘                  └─────────────┘
```

**Any node can receive any request** — if it doesn't own the key, it forwards  
the request to the correct primary node transparently.

---

## Key Concepts Implemented

### 1. Consistent Hashing (`ConsistentHashRing.java`)
Keys are mapped to nodes on a logical ring. Adding/removing a node only  
reshuffles ~1/N keys instead of everything. Each node gets 150 virtual nodes  
for even distribution.

### 2. Replication (`ReplicationService.java`)
After a primary node writes a key, it fans out the write to (replicationFactor − 1)  
replica nodes in parallel using a thread pool. Default replication factor = 2.

### 3. Write-Ahead Log (`WriteAheadLog.java`)
Every PUT and DELETE is flushed to disk **before** the in-memory map is updated.  
On crash/restart, the log is replayed in `@PostConstruct` to restore state.

### 4. Leader Failover (`HealthCheckScheduler.java`)
Every 3 seconds each node pings its peers via `/actuator/health`. A node that  
stops responding is removed from the hash ring — keys are automatically rerouted  
to the next live node on the ring.

### 5. Benchmarking (`AdminController.java` — `/admin/benchmark`)
Concurrent PUT+GET load is issued from a thread pool. Reports:
- Total duration
- Throughput (ops/sec)
- Average latency per operation

---

## Running the Cluster

```bash
# Build and start all 3 nodes
docker compose up --build

# Wait ~20s for startup, then try it out:

# Store a value (can hit any node)
curl -X PUT http://localhost:8081/kv/hello -H "Content-Type: text/plain" -d "world"

# Read from a different node (transparent routing)
curl http://localhost:8082/kv/hello

# Delete
curl -X DELETE http://localhost:8083/kv/hello
```

---

## Admin Endpoints

```bash
# Cluster health (which nodes are alive)
curl http://localhost:8081/admin/status

# Which nodes own a specific key?
curl http://localhost:8081/admin/ring/hello

# All keys stored on node-1 locally
curl http://localhost:8081/admin/keys

# Run a benchmark: 1000 ops, 20 concurrent threads
curl -X POST "http://localhost:8081/admin/benchmark?ops=1000&threads=20"
```

---

## Testing Fault Tolerance

```bash
# Kill node-2
docker compose stop node-2

# Health check will detect it within 3 seconds
curl http://localhost:8081/admin/status

# Operations still succeed — keys rerouted to surviving nodes
curl -X PUT http://localhost:8081/kv/resilient -H "Content-Type: text/plain" -d "yes"

# Bring node-2 back — it rejoins the ring automatically
docker compose start node-2
```

---

## Testing WAL Recovery

```bash
# Write some data
curl -X PUT http://localhost:8081/kv/durable -H "Content-Type: text/plain" -d "survives-crash"

# Restart node-1 (simulates crash)
docker compose restart node-1

# Data is restored from WAL
curl http://localhost:8081/kv/durable   # → "survives-crash"
```

---

## Project Structure

```
src/
├── hashing/
│   └── ConsistentHashRing.java     # Virtual-node ring, key → node mapping
├── wal/
│   └── WriteAheadLog.java          # Disk persistence + crash recovery
├── replication/
│   └── ReplicationService.java     # Parallel fan-out to replica nodes
├── health/
│   └── HealthCheckScheduler.java   # Peer ping + ring update on failure
├── service/
│   ├── StorageService.java         # ConcurrentHashMap store + WAL integration
│   └── RouterService.java          # Routes to primary or forwards to peer
├── controller/
│   ├── KvController.java           # Public REST API  (GET/PUT/DELETE /kv/{key})
│   ├── InternalReplicationController.java  # Receives replicated writes
│   └── AdminController.java        # Status, ring info, benchmark
└── config/
    └── NodeRegistry.java           # Peer discovery and address book
```

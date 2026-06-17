# Directory-Based Cache Coherence Protocol (Write-Update)

This repository contains a heterogeneous, distributed multi-node system demonstrating a distributed shared virtual memory space regulated by a **Directory-Based Cache Coherence Protocol** using a **Write-Update** strategy. 

The cluster consists of three independent nodes operating across different operating systems and languages:
* **Node 1 (Linux):** C# / .NET framework
* **Node 2 (macOS):** Python / FastAPI framework
* **Node 3 (Windows):** Java / Spring Boot framework

## Architecture & Protocols

The cluster uses a Peer-to-Peer (P2P) network model with a dynamically elected **Home Node (Leader)** acting as the authoritative coordinator.

### 1. Cache Coherence (Write-Update)
* **Home Node (Directory Manager):** Active exclusively on the current leader. It maintains `mainMemory` (global variable states) and `presenceList` (a tracking map defining which sub-nodes possess duplicates of specific data keys).
* **Write-Update Implementation:** When any node alters a variable locally, it dispatches an HTTP `POST` `/update-request` to the Home Node. Instead of invalidating peer caches, the Home Node immediately broadcasts the `newValue` to all active nodes tracking that resource via `POST` `/force-update`, ensuring synchronous, cluster-wide data consistency.

### 2. Fault Tolerance
* **Failure Detection (Heartbeats):** Nodes continuously transmit lightweight UDP heartbeat signals down network channels to discover active peers and detect deadlocks or process crashes via a 5-second timeout window.
* **Leader Election (Bully Algorithm):** If the Home Node fails, the remaining active nodes launch a Bully election protocol via HTTP `POST` `/election`. Nodes are assigned static IDs (`Node 1` < `Node 2` < `Node 3`). The operational node with the highest priority ID automatically assumes the Home Node authority role.

---

## Component Specifications

### Endpoints (HTTP/REST)
* `POST /update-request`: Triggered by clients or forwarded via proxy to commit data changes.
* `POST /force-update`: Triggered by the Leader to enforce synchronous data overwrites on standard nodes.
* `POST /election`: Inter-node channel handler for orchestrating Bully election states.
* `GET /reconstruct-directory`: Invoked by a newly elected leader to retrieve local cache structures from active peers to reconstruct the global `presenceList` mapping state.

---

## Deployment & Network Configuration

The final presentation is conducted across three physically distinct machines. Nodes must be configured using the actual local network IP addresses assigned to each operating system.

### Target Network Matrix (Production Deployment)

| Node ID | Operating System | Technology | HTTP Port |
| :--- | :--- | :--- | :--- |
| **Node 1** | Linux | C# / .NET | `8081` |
| **Node 2** | macOS | Python / FastAPI | `8082` |
| **Node 3** | Windows | Java / Spring Boot | `8083` |
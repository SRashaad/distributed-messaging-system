# Fault-Tolerant Distributed Messaging System

Course prototype: a **leader-based, strongly consistent** distributed messaging cluster in **Go**, with **ZooKeeper-backed leader election**, **gRPC** transport, **quorum replication**, **Lamport logical clocks**, and a **web dashboard** (HTTP bridge + live UI).

---

## Submission package (zip contents)

When you submit, include in **one zip file**:

| Item | Notes |
| :--- | :--- |
| **Report** | PDF or document as required by your module — draft: **[`docs/COURSE_REPORT.md`](docs/COURSE_REPORT.md)** (export to PDF if needed) |
| **Source code** | This repository (excluding large local-only folders such as `bin/`, `.tools/`, `.cluster-logs/` if present) |
| **Presentation** | Slides + any **text files** with **links** (e.g. video links), as required |
| **This `README.md`** | At the **root** of the project next to `go.mod` |

---

## Team members

| Name | Registration No. | Email | Primary focus |
| :--- | :--- | :--- | :--- |
| Imansa Bodini | IT24101844 | pedhuruarachchigepremarathne@gmail.com | Fault tolerance |
| Senul Mintharu | IT24101497 | senulmintharu@gmail.com | Data replication & consistency |
| Sabeelur Rashaad | IT24100146 | it24100146@my.sliit.lk | Time synchronization |
| Vimukthi Herath | IT24101500 | vimukthiherath123@gmail.com | Consensus & agreement |

---

## Prerequisites

- **Go** 1.21 or newer (see `go.mod` for the toolchain used in this repo)
- **Apache ZooKeeper** reachable at **`localhost:2181`** (default used by the servers), with **Java (JDK 17+)** if you run ZK from the official distribution  
- Optional: **protoc** + `protoc-gen-go` + `protoc-gen-go-grpc` only if you change `.proto` files and regenerate code (`make proto`)

---

## How to run the prototype

### 1. Start ZooKeeper

Nodes expect ZooKeeper on **`localhost:2181`** unless you override `-zk`.

- **Step-by-step (Windows + `zkServer.cmd`)**: see **[`docs/running_the_cluster_guide.md`](docs/running_the_cluster_guide.md)** — Step 1.
- **Docker (quick option)**:

  ```bash
  docker run -d --name zk -p 2181:2181 zookeeper:3.9
  ```

### 2. Dependencies

From the repository root:

```bash
go mod tidy
```

### 3. Start the three cluster nodes

**Option A — one command, separate windows (Windows PowerShell)**

From the repo root:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned   # only if script execution is blocked
.\scripts\start-cluster.ps1
```

Optional: `-NodeCount 5` for five nodes; `-UseBinary .\bin\server.exe` after `go build -o bin/server.exe ./cmd/server`.

**Option B — three terminals manually**

Use the exact commands in **[`docs/running_the_cluster_guide.md`](docs/running_the_cluster_guide.md)** (Step 3).  
Equivalent short form:

```bash
go run ./cmd/server -id node1 -port 5001 -peers "localhost:5002,localhost:5003" -zk "localhost:2181"
go run ./cmd/server -id node2 -port 5002 -peers "localhost:5001,localhost:5003" -zk "localhost:2181"
go run ./cmd/server -id node3 -port 5003 -peers "localhost:5001,localhost:5002" -zk "localhost:2181"
```

### 4. (Optional) Web dashboard

From the repo root:

```bash
go run ./cmd/dashboard -http localhost:8090 -nodes "localhost:5001,localhost:5002,localhost:5003"
```

Open **http://localhost:8090** in a browser.  
On some Windows setups **port 8080** is reserved; **8090** (or another free port) avoids bind errors.

### 5. (Optional) CLI client

```bash
go run ./cmd/client -leader localhost:5001 -action publish -message "Hello"
go run ./cmd/client -leader localhost:5001 -action consume
```

Adjust `-leader` to a **live** node port if the leader has moved after failover.

### 6. Build binaries (optional)

```bash
go build -o bin/server ./cmd/server
go build -o bin/dashboard ./cmd/dashboard
go build -o bin/client ./cmd/client
```

On Windows, executables are named `server.exe`, `dashboard.exe`, `client.exe` under `bin/`.

### 7. Tests

```bash
go test ./...
```

Integration tests may expect a running cluster; see `test/integration/` and your module brief.

---

## Fault-tolerance demo (short)

With all three nodes running, identify the terminal that logs **`★ Became LEADER`**, stop that process with **Ctrl+C**, and watch the remaining nodes: a new leader should be elected after ZooKeeper observes the session/ephemeral node drop.  
More detail: **[`docs/running_the_cluster_guide.md`](docs/running_the_cluster_guide.md)** — Step 4.

---

## Further documentation (same repository)

| Document | Purpose |
| :--- | :--- |
| [`pitch.md`](pitch.md) | Video / presentation script (by team member; aligned to this codebase) |
| [`docs/COURSE_REPORT.md`](docs/COURSE_REPORT.md) | Main project report (Markdown; export to PDF for submission if required) |
| [`docs/running_the_cluster_guide.md`](docs/running_the_cluster_guide.md) | Evaluator-oriented run guide (ZK, 3 nodes, kill leader, CLI) |
| [`docs/PROJECT_README_EXTENDED.md`](docs/PROJECT_README_EXTENDED.md) | **Full** original README: architecture, modules, diagrams, branching, long “How to Run” |
| [`docs/STARTER_GUIDE.md`](docs/STARTER_GUIDE.md) | Onboarding and tooling for contributors |
| [`docs/member1_fault_tolerance.md`](docs/member1_fault_tolerance.md) | Fault tolerance module |
| [`docs/member2_replication.md`](docs/member2_replication.md) | Replication module |
| [`docs/member3_timesync.md`](docs/member3_timesync.md) | Time sync module |
| [`docs/member4_consensus.md`](docs/member4_consensus.md) | Consensus module |
| [`documentation.md`](documentation.md) | High-level module overview |

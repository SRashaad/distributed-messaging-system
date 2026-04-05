# 🚀 Distributed Messaging System — Evaluator's Run Guide

Welcome to the Distributed Messaging System! This project has been wired together using **Apache ZooKeeper** to handle robust, ephemeral-session-based Leader Election, replacing manual Raft timeouts.

If you are a lecturer, evaluator, or a team member seeing this system for the first time, this guide explains exactly how to run and verify it on your local machine.

---

## 🛠️ Prerequisites

Before you begin, ensure you have the following installed on your machine:

1. **Go (1.21+)**: Used to compile and run the cluster nodes.
2. **Java (JDK 17+)**: Required to run Apache ZooKeeper on your local machine.
3. **Apache ZooKeeper (3.9.x)**: The coordination server.

---

## 🐘 Step 1: Start Apache ZooKeeper

The distributed nodes rely on a running ZooKeeper ensemble to negotiate who the Leader is.

**If you are on Windows:**
1. Open PowerShell and navigate to your ZooKeeper installation directory (e.g., `C:\zookeeper\apache-zookeeper-3.9.5-bin\`).
2. Make sure your Java path is accessible. If not, set it temporarily:
   ```powershell
   $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
   ```
3. Run the ZooKeeper startup script:
   ```powershell
   .\bin\zkServer.cmd
   ```

*You should see output indicating that ZooKeeper is binding to `0.0.0.0:2181`. Leave this terminal open!*

---

## 🖥️ Step 2: Download Go Dependencies

Because the project now natively imports `go-zookeeper/zk`, we need to fetch the packages.

1. Open a new terminal.
2. Navigate to the root of the project: `cd d:\distributed-messaging-system`
3. Download the dependencies:
   ```powershell
   go mod tidy
   ```

*(If you are using **JetBrains GoLand**, open `go.mod` and simply click the "Sync Dependencies" prompt at the top of the file.)*

---

## 🌐 Step 3: Boot the Cluster (Start 3 Nodes)

We are going to spin up a 3-node cluster. Each node runs in its own terminal window (or IDE tab) and listens on a different TCP port.

Open **3 separate terminal windows** inside `d:\distributed-messaging-system`, and run one command in each:

**Terminal 1 (Node 1):**
```powershell
go run cmd/server/main.go --id node1 --port 5001 --peers "localhost:5002,localhost:5003" --zk "localhost:2181"
```

**Terminal 2 (Node 2):**
```powershell
go run cmd/server/main.go --id node2 --port 5002 --peers "localhost:5001,localhost:5003" --zk "localhost:2181"
```

**Terminal 3 (Node 3):**
```powershell
go run cmd/server/main.go --id node3 --port 5003 --peers "localhost:5001,localhost:5002" --zk "localhost:2181"
```

### What happens next?
* All 3 nodes instantly connect to ZooKeeper via the `--zk` flag.
* They each create an Ephemeral Sequential znode under `/election` (e.g., `/election/node-0001`).
* **The node with the lowest sequence number will instantly announce itself as the LEADER.** Look at the terminal logs — one of them will print:
  `[nodeX] ★ Became LEADER (term=1)`
* The other two nodes will print:
  `[nodeY] State=Follower, watching /election/node-XXXX`

---

## 💣 Step 4: Test Fault Tolerance (Kill the Leader)

The core mechanism of a distributed system is surviving crashes. Let's prove it works!

1. Look at your 3 terminals and find the one that says `★ Became LEADER`.
2. Click into that terminal and press **`Ctrl + C`** to crash that node instantly.
3. Look at the remaining two terminals.
4. Because the dead node's ZooKeeper session expired, its ephemeral znode was instantly deleted.
5. The Follower who was watching that znode gets an alert, looks at the list, realizes it is now the lowest sequence number, and prints:
   `[nodeZ] ★ Became LEADER (term=2)`

**The cluster successfully recovered without any manual intervention!**

*You can now restart the dead node using its original startup command, and it will politely rejoin the cluster as a Follower.*

---

## 📨 Step 5: Interact using the Client

Finally, let's use the CLI tool to publish a message into the cluster.

Open a **4th terminal window**, and run the Client app:
*(Be sure to change `--leader "localhost:XXXX"` to whichever port belongs to your active Leader!)*

```powershell
# Publish a message to the Leader
go run cmd/client/main.go --leader "localhost:5002" --action publish --message "Hello Distributed World"

# Consume the message back
go run cmd/client/main.go --leader "localhost:5002" --action consume
```
---
go run ./cmd/dashboard -http localhost:8090 -nodes "localhost:5001,localhost:5002,localhost:5003"
---
*(Note: While the CLI correctly dials the leader, full message propagation awaits the completion of the gRPC MessagingService protobuf handlers by the transport team module.)*

---

🎉 **You have successfully deployed and tested a ZooKeeper-backed Go Distributed System!**

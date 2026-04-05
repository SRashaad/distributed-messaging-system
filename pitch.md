# SC 2062 — Demo pitch script (video / presentation)

This document mirrors the **structure and pacing** of a senior-team style walkthrough, but every technical claim below matches **our** codebase: **Go**, **gRPC**, **Apache ZooKeeper** for leader election, **Raft-inspired log replication** (AppendEntries, terms, quorum), **Lamport logical time** for ordering, and a **Go HTTP dashboard** (static HTML/JS + REST bridge — not Python, React, SQLite, or NTP).

**How to use:** Each block is a **speaker script**. Optional slide titles appear in *italics*. Adjust names if one person does the intro.

---

## 1. Opening — *Presenter* (whole team or one host, ~1–2 min)

Hi, we’re **[Name 1]** and welcome. Today we’re demonstrating our **fault-tolerant distributed messaging system** for **SC 2062**.

Modern data platforms stay available because of a few core ideas: **fault tolerance**, **agreement on who is the leader**, **replicated state**, and a **clear rule for ordering events** when wall clocks disagree. Our project brings those ideas together in a small but real cluster.

This was a team effort:

- **Imansa Bodini** led our **fault-tolerance** work: failure detection, recovery paths, and keeping the cluster understandable when nodes disappear.
- **Senul Mintharu** implemented **data replication and consistency**: quorum replication, the replicated log, and safe behaviour under leader and follower failures.
- **Sabeelur Rashaad** built the **time-synchronization** story using **Lamport logical clocks** — so we get **causal ordering** without pretending we have perfect NTP everywhere.
- **Vimukthi Herath** owns **consensus and agreement**: wiring **ZooKeeper-backed leader election** with our **Raft-style log and RPCs** in Go.

We built a **leader-based** system: **ZooKeeper** picks a stable leader; the leader **replicates** each message to followers; a **quorum** must acknowledge before we treat a write as committed; **Lamport timestamps** order messages on reads; clients talk **gRPC**; and we ship a **lightweight web dashboard** so you can *see* roles, logs, and redirects in real time.

Our stack is **Go**, **gRPC / Protocol Buffers**, **Apache ZooKeeper**, and an **HTTP bridge** for the browser — not Python, AIOHTTP, SQLite, or React.

Over the next few minutes, each of us will walk through one layer, then we’ll tie it together.

---

## 2. Architecture & data path — *Vimukthi Herath* (consensus & agreement, ~2–3 min)

*Slide: Cluster topology — three (or five) identical nodes*

At the heart of our system is a **small cluster of peer nodes**. Each process runs our **server** binary and exposes **gRPC** on its own port — for example **5001, 5002, 5003**.

**Leader election** is handled by **Apache ZooKeeper**: every node creates an **ephemeral sequential** znode under a shared election path. The **ordering** of those znodes determines who is leader — similar in *outcome* to Raft’s idea of “one leader at a time,” but the **election mechanism** is **ZooKeeper**, not a second full Raft implementation inside Go.

Each node still runs **Raft-inspired** logic for **terms**, **log indices**, and **AppendEntries**-style RPCs for replication — that’s how we keep the **log** consistent across peers.

We typically demo a **three-node** cluster: if **one** node fails, the **remaining two** can still form a **majority** for quorum in a three-replica setup — so the design matches classic HA thinking.

*Slide: End-to-end publish path*

A **client** — our **CLI** or the **dashboard bridge** — sends a **Publish** RPC to a node’s gRPC address.

- If that node is a **follower**, our node logic does **not** silently accept the write. The client gets a clear **“not the leader — redirect to …”** style response. The **CLI** and **dashboard** then **redial the leader** using the live cluster view. So the user doesn’t manually reconfigure ports — same *idea* as an HTTP redirect, but it’s **gRPC + explicit handling**, not HTTP 307.
- On the **leader**, we assign a **Lamport timestamp**, append to the **replicated log**, send **AppendEntries** to followers, and **wait for quorum**. Only then do we apply to the **message store** and return success.

Committed entries are what we show in the **dashboard log table** — sorted by **logical time**, then **log index** as a tie-breaker — so ordering stays deterministic even when messages arrive out of order at different replicas.

---

## 3. Fault tolerance — *Imansa Bodini* (~2–3 min)

*Slide: Why nodes fail; what “available” means*

Our goal was straightforward: when **nodes crash** or **networks glitch**, the cluster should **keep a clear story**: who is the leader, who is alive, and how do **rejoining** nodes catch up.

We use **heartbeat-style monitoring** and **recovery** hooks in **`internal/fault`**: the leader path emits **heartbeats** through the replication path; followers run **failure detection** with configurable timeouts from **`internal/config`** (heartbeat and election windows you can tune from flags).

When **ZooKeeper** sees a session end — for example when you **Ctrl+C** the leader — the **ephemeral election znode** disappears. **Watchers** wake up on the other nodes; they **re-run election**; a **new leader** appears. That’s our **automatic failover** story: it’s driven by **coordination in ZooKeeper** plus our **consensus state machine**, not by a separate HTTP layer.

*Slide: Dashboard — healthy vs dead node*

In our **dashboard**, you don’t get React widgets — you get a **clean dark UI** that **polls** each node’s **GetStatus** RPC: **node ID**, **gRPC address**, **role** (follower, **Electing…** when in a candidate-style state, or leader with a **crown**), **term**, **log length**. If a process is gone, that card goes **grey / unreachable** — so failover is **visible** without reading three terminals at once.

When a node **comes back**, our **recovery** path is designed to **sync missed log entries** over **gRPC** so the node converges — we don’t claim sub-second SLAs unless we **measure** them on your laptop; the **mechanism** is there for the demo.

---

## 4. Data replication & consistency — *Senul Mintharu* (~2–3 min)

*Slide: Leader-based replication*

Replication is **leader-based**: only the **leader** initiates new log entries for client writes. Followers **append** what the leader sends and participate in **quorum**.

We implement **synchronous quorum replication** in the sense that matters for **strong consistency** in the demo: the leader calls **`WaitForQuorum`** before returning success to the client. If followers don’t acknowledge in time, the publish **fails cleanly** instead of lying about durability.

We also added **deduplication** on the leader path: if the **same payload** was already stored, we **short-circuit** instead of burning another round of quorum work — that’s our practical answer to duplicate sends.

*Slide: Where data lives*

Our primary demo path uses an **in-memory replicated log** and **in-memory message store** for speed and simplicity. We have **storage abstractions** for richer persistence experiments; we do **not** claim SQLite-backed durability in the default path — that honesty matters for academic integrity.

*Slide: Dashboard — replicated log table*

The dashboard’s **bottom-right** panel shows **logical timestamp**, **Raft index**, and **message body** — proving that **ordering** follows our **Lamport + index** rule, not random map iteration.

---

## 5. Time synchronization — *Sabeelur Rashaad* (~2 min)

*Slide: Why wall clocks aren’t enough*

In distributed systems, **you cannot assume** all machines share one true clock. Our module doesn’t implement **NTP** or display **offset charts** in the UI — instead we implement **Lamport’s logical clock** rules.

On **publish**, the leader **ticks** the clock. When **AppendEntries** arrives at a follower, we **`Update`** the local clock to respect **happens-before** relationships. On **Consume**, we **sort** by **timestamp**, then **index** — so clients see a **stable causal order** even when network delay reorders delivery.

*Slide: Dashboard — logical time column*

You can point to the **“Logical time”** column in the dashboard and explain: this is **not** “NTP accuracy in milliseconds” — it’s **logical time** for **ordering**, which is the standard textbook approach when you don’t control the universe’s clocks.

---

## 6. Consensus recap & demo flow — *Vimukthi Herath* (~1–2 min)

*Slide: What “consensus” means here*

**Consensus** in our project means: **one leader at a time** (via **ZooKeeper**), **one ordered log** we all agree to append (via **AppendEntries** and **quorum**), and **safe redirects** when clients hit the wrong node.

*Slide: Live demo checklist*

Suggested live sequence:

1. Show **ZooKeeper** running on **2181**.
2. Start **three servers** (or run **`scripts/start-cluster.ps1`** on Windows).
3. Open the **dashboard** on a free port (e.g. **8090** if **8080** is blocked on Windows).
4. Show **one leader**, **two followers**, then **kill the leader** in its terminal — watch **another** node become leader on the next poll.
5. Publish from a **follower** port in the simulator — show the **trace** line that says you **redirected to the leader**.

*Slide: Closing*

To wrap up: we combined **ZooKeeper election**, **gRPC messaging**, **quorum replication**, **Lamport ordering**, and **fault-tolerance hooks** into one coherent **Go** codebase — with a **dashboard** that makes the behaviour visible. Thank you — we’re happy to take questions.

---

## 7. Optional — full-team Q&A cheat sheet (30 seconds)

| Topic | One-line answer |
| :--- | :--- |
| Why ZooKeeper + “Raft” in the slides? | **Election** is **ZooKeeper**; **log replication** follows **Raft-style** RPCs and state. |
| Production-ready? | **Course prototype** — insecure gRPC for localhost, in-memory default stores. |
| React / SQLite / Python? | **No** — **Go**, **gRPC**, optional **file** storage experiments, **vanilla** dashboard. |
| NTP? | **No** — **Lamport** logical clocks for **ordering**. |

---

## Revision log

- Align this script with **`README.md`**, **`docs/running_the_cluster_guide.md`**, and the actual packages under **`internal/`** before recording.
- Replace **[Name 1]** with whoever does the intro; rehearse handoffs between the four named sections.

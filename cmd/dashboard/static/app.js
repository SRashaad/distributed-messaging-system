const POLL_MS = 1000;

const clusterEl = document.getElementById("cluster-cards");
const logBody = document.getElementById("log-body");
const targetSelect = document.getElementById("target-node");
const msgInput = document.getElementById("msg-input");
const publishBtn = document.getElementById("publish-btn");
const consoleEl = document.getElementById("console");

let lastCluster = [];

/** Dial targets from GET /api/config (dashboard -nodes); used until first /api/cluster succeeds */
let bootstrapNodes = [
  "localhost:5001",
  "localhost:5002",
  "localhost:5003",
  "localhost:5004",
  "localhost:5005",
];

async function loadConfig() {
  try {
    const res = await fetch("/api/config");
    const data = await res.json();
    if (Array.isArray(data.nodes) && data.nodes.length) {
      bootstrapNodes = data.nodes;
      syncTargetDropdown([]);
    }
  } catch (e) {
    console.error(e);
  }
}

function roleClass(n) {
  if (!n.reachable) return "dead";
  return n.role || "follower";
}

function renderCluster(nodes) {
  lastCluster = nodes;
  clusterEl.innerHTML = "";
  nodes.forEach((n) => {
    const card = document.createElement("article");
    const rc = roleClass(n);
    card.className = `node-card ${rc}`;

    const title = document.createElement("div");
    title.className = "title-row";
    const name = document.createElement("div");
    name.className = "name";
    if (n.reachable) {
      name.textContent = n.node_id || n.grpc_listen_address;
    } else {
      name.textContent = n.grpc_listen_address;
    }
    title.appendChild(name);
    if (n.reachable && n.role === "leader") {
      const crown = document.createElement("span");
      crown.className = "crown";
      crown.textContent = "👑";
      crown.setAttribute("aria-label", "Leader");
      title.appendChild(crown);
    }
    card.appendChild(title);

    const dl = document.createElement("dl");

    const addRow = (label, value) => {
      const dt = document.createElement("dt");
      dt.textContent = label;
      const dd = document.createElement("dd");
      dd.textContent = value;
      dl.appendChild(dt);
      dl.appendChild(dd);
    };

    if (n.reachable) {
      addRow("Node ID", n.node_id || "—");
      addRow("gRPC address", n.grpc_listen_address || "—");
      addRow("Term / log", `${n.current_term} / len ${n.log_length}`);
      addRow("Commit", String(n.commit_index ?? "—"));
      addRow("Known leader", n.leader_id || "—");
    } else {
      addRow("Status", "Unreachable");
      addRow("Dial target", n.grpc_listen_address);
      if (n.error) addRow("Error", n.error);
    }

    card.appendChild(dl);

    const pill = document.createElement("span");
    pill.className = "role-pill";
    pill.textContent = n.reachable ? n.role_display || n.role : "Dead";
    card.appendChild(pill);

    clusterEl.appendChild(card);
  });

  syncTargetDropdown(nodes);
}

function syncTargetDropdown(nodes) {
  const prev = targetSelect.value;
  targetSelect.innerHTML = "";
  const fromCluster = nodes.length
    ? nodes.map((n) => n.grpc_listen_address).filter(Boolean)
    : [];
  const addrs = fromCluster.length ? fromCluster : bootstrapNodes;
  const unique = [...new Set(addrs)];
  unique.forEach((addr) => {
    const opt = document.createElement("option");
    opt.value = addr;
    opt.textContent = addr;
    targetSelect.appendChild(opt);
  });
  if (prev && unique.includes(prev)) targetSelect.value = prev;
}

function renderLog(entries) {
  logBody.innerHTML = "";
  entries.forEach((e) => {
    const tr = document.createElement("tr");
    const tdTs = document.createElement("td");
    tdTs.className = "mono";
    tdTs.textContent = String(e.timestamp);
    const tdIx = document.createElement("td");
    tdIx.className = "mono";
    tdIx.textContent = String(e.index);
    const tdData = document.createElement("td");
    tdData.textContent = e.data;
    tr.appendChild(tdTs);
    tr.appendChild(tdIx);
    tr.appendChild(tdData);
    logBody.appendChild(tr);
  });
}

async function tick() {
  try {
    const res = await fetch("/api/cluster");
    const data = await res.json();
    if (Array.isArray(data)) renderCluster(data);
  } catch (e) {
    console.error(e);
  }
  try {
    const res = await fetch("/api/log");
    if (res.ok) {
      const data = await res.json();
      renderLog(data.entries || []);
    }
  } catch (e) {
    console.error(e);
  }
}

publishBtn.addEventListener("click", async () => {
  const target = targetSelect.value;
  const message = msgInput.value.trim();
  if (!message) {
    consoleEl.textContent = "Enter a message.";
    return;
  }
  publishBtn.disabled = true;
  try {
    const res = await fetch("/api/publish", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target, message }),
    });
    const data = await res.json();
    const lines = (data.trace || []).join("\n");
    const tail = data.success
      ? `\n→ index ${data.index}, Lamport ${data.timestamp}`
      : `\n✗ ${data.error || "failed"}`;
    consoleEl.textContent = lines + tail;
  } catch (e) {
    consoleEl.textContent = String(e);
  } finally {
    publishBtn.disabled = false;
  }
});

(async () => {
  await loadConfig();
  setInterval(tick, POLL_MS);
  tick();
})();

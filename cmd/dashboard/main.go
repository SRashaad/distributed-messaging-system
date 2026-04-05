// Dashboard bridge: serves the static UI and REST JSON that maps to cluster gRPC (GetStatus, Publish, Consume).
package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distributed-messaging-system/internal/transport/proto"
)

//go:embed static/*
var staticRoot embed.FS

func main() {
	httpAddr := flag.String("http", "localhost:8080", "HTTP listen address for dashboard + API")
	nodes := flag.String("nodes", "localhost:5001,localhost:5002,localhost:5003,localhost:5004,localhost:5005",
		"comma-separated gRPC addresses (order = cards left to right; use 3–5+ nodes as you run)")
	flag.Parse()

	cluster := splitAndTrim(*nodes)
	if len(cluster) == 0 {
		log.Fatal("no cluster nodes configured (-nodes)")
	}

	staticFS, err := fs.Sub(staticRoot, "static")
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(staticFS)))
	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"nodes": cluster})
	})
	mux.HandleFunc("GET /api/cluster", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, clusterSnapshot(r.Context(), cluster))
	})
	mux.HandleFunc("GET /api/log", func(w http.ResponseWriter, r *http.Request) {
		rows, err := fetchSortedLog(r.Context(), cluster)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, map[string]any{"entries": rows})
	})
	mux.HandleFunc("POST /api/publish", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Target  string `json:"target"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if body.Target == "" || body.Message == "" {
			http.Error(w, "target and message required", http.StatusBadRequest)
			return
		}
		res := publishWithTrace(r.Context(), cluster, body.Target, []byte(body.Message))
		writeJSON(w, res)
	})

	log.Printf("dashboard listening on http://%s (cluster %v)", *httpAddr, cluster)
	if err := http.ListenAndServe(*httpAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

type clusterNodeJSON struct {
	Reachable           bool   `json:"reachable"`
	GrpcListenAddress   string `json:"grpc_listen_address"`
	NodeID              string `json:"node_id,omitempty"`
	Role                string `json:"role,omitempty"`
	RoleDisplay         string `json:"role_display,omitempty"`
	CurrentTerm         uint64 `json:"current_term,omitempty"`
	LogLength           uint64 `json:"log_length,omitempty"`
	LeaderID            string `json:"leader_id,omitempty"`
	CommitIndex         uint64 `json:"commit_index,omitempty"`
	Error               string `json:"error,omitempty"`
}

func clusterSnapshot(ctx context.Context, addrs []string) []clusterNodeJSON {
	out := make([]clusterNodeJSON, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, dialStatus(ctx, addr))
	}
	return out
}

func dialStatus(ctx context.Context, addr string) clusterNodeJSON {
	cctx, cancel := context.WithTimeout(ctx, 900*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(cctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return clusterNodeJSON{
			Reachable:         false,
			GrpcListenAddress: addr,
			Error:             err.Error(),
		}
	}
	defer conn.Close()

	client := proto.NewMessagingServiceClient(conn)
	st, err := client.GetStatus(cctx, &proto.StatusRequest{})
	if err != nil {
		return clusterNodeJSON{
			Reachable:         false,
			GrpcListenAddress: addr,
			Error:             err.Error(),
		}
	}
	role, display := roleForUI(st.GetRole())
	return clusterNodeJSON{
		Reachable:           true,
		GrpcListenAddress:   st.GetGrpcListenAddress(),
		NodeID:              st.GetNodeId(),
		Role:                role,
		RoleDisplay:         display,
		CurrentTerm:         st.GetCurrentTerm(),
		LogLength:           st.GetLogLength(),
		LeaderID:            st.GetLeaderId(),
		CommitIndex:         st.GetCommitIndex(),
	}
}

func roleForUI(r proto.NodeRole) (key string, display string) {
	switch r {
	case proto.NodeRole_NODE_ROLE_LEADER:
		return "leader", "Leader"
	case proto.NodeRole_NODE_ROLE_CANDIDATE:
		return "candidate", "Electing…"
	default:
		return "follower", "Follower"
	}
}

type logRowJSON struct {
	Timestamp uint64 `json:"timestamp"`
	Index     uint64 `json:"index"`
	Data      string `json:"data"`
	Term      uint64 `json:"term"`
}

func fetchSortedLog(ctx context.Context, cluster []string) ([]logRowJSON, error) {
	var leaderAddr string
	for _, addr := range cluster {
		s := dialStatus(ctx, addr)
		if !s.Reachable {
			continue
		}
		if s.Role == "leader" {
			leaderAddr = s.GrpcListenAddress
			break
		}
	}
	if leaderAddr == "" {
		for _, addr := range cluster {
			s := dialStatus(ctx, addr)
			if s.Reachable {
				leaderAddr = s.GrpcListenAddress
				break
			}
		}
	}
	if leaderAddr == "" {
		return nil, fmt.Errorf("no reachable node to read log")
	}

	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(cctx, leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := proto.NewMessagingServiceClient(conn)
	resp, err := client.Consume(cctx, &proto.ConsumeRequest{FromIndex: 1})
	if err != nil {
		return nil, err
	}
	rows := make([]logRowJSON, 0, len(resp.GetMessages()))
	for _, m := range resp.GetMessages() {
		rows = append(rows, logRowJSON{
			Timestamp: m.GetTimestamp(),
			Index:     m.GetIndex(),
			Data:      string(m.GetData()),
			Term:      m.GetTerm(),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Timestamp == rows[j].Timestamp {
			return rows[i].Index < rows[j].Index
		}
		return rows[i].Timestamp < rows[j].Timestamp
	})
	return rows, nil
}

type publishResult struct {
	Success   bool     `json:"success"`
	Trace     []string `json:"trace"`
	Index     uint64   `json:"index,omitempty"`
	Timestamp uint64   `json:"timestamp,omitempty"`
	Error     string   `json:"error,omitempty"`
}

func publishWithTrace(ctx context.Context, cluster []string, target string, payload []byte) publishResult {
	trace := []string{fmt.Sprintf("Publish via %s", target)}
	res, err := tryPublish(ctx, target, payload)
	if err == nil {
		trace = append(trace, "Success")
		return publishResult{Success: true, Trace: trace, Index: res.GetIndex(), Timestamp: res.GetTimestamp()}
	}
	trace = append(trace, fmt.Sprintf("Failed on %s: %v", target, err))

	if !strings.Contains(strings.ToLower(err.Error()), "not the leader") {
		return publishResult{Success: false, Trace: trace, Error: err.Error()}
	}

	leaderAddr := findLeaderAddress(ctx, cluster)
	if leaderAddr == "" {
		trace = append(trace, "Could not discover leader via GetStatus")
		return publishResult{Success: false, Trace: trace, Error: err.Error()}
	}
	if leaderAddr == target {
		trace = append(trace, "Leader address matches target; giving up")
		return publishResult{Success: false, Trace: trace, Error: err.Error()}
	}

	trace = append(trace, fmt.Sprintf("Redirecting to leader at %s", leaderAddr))
	res2, err2 := tryPublish(ctx, leaderAddr, payload)
	if err2 != nil {
		trace = append(trace, fmt.Sprintf("Failed on leader %s: %v", leaderAddr, err2))
		return publishResult{Success: false, Trace: trace, Error: err2.Error()}
	}
	trace = append(trace, "Success")
	return publishResult{Success: true, Trace: trace, Index: res2.GetIndex(), Timestamp: res2.GetTimestamp()}
}

func tryPublish(ctx context.Context, addr string, payload []byte) (*proto.PublishResponse, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(cctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := proto.NewMessagingServiceClient(conn)
	return client.Publish(cctx, &proto.PublishRequest{Message: payload})
}

func findLeaderAddress(ctx context.Context, cluster []string) string {
	for _, addr := range cluster {
		s := dialStatus(ctx, addr)
		if s.Reachable && s.Role == "leader" && s.GrpcListenAddress != "" {
			return s.GrpcListenAddress
		}
	}
	return ""
}

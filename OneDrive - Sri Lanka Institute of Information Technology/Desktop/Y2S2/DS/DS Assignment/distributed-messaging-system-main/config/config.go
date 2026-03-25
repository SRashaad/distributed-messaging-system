// =============================================================================
// Module: Configuration
// File: config/config.go
// Responsible Member: All Members (Shared)
// Purpose: Defines the configuration structure for a node in the cluster.
//          All tunable parameters (timeouts, ports, peer addresses) are
//          centralized here. The config is parsed from command-line flags
//          in cmd/server/main.go and passed to internal/node.New().
//
// Connections:
//   - Created in cmd/server/main.go from command-line flags.
//   - Passed to internal/node.New() to initialize all modules.
//   - Timeout values are used by:
//       * internal/fault/detector.go    → HeartbeatTimeout()
//       * internal/fault/heartbeat.go   → HeartbeatInterval()
//       * internal/consensus/election.go → ElectionMinTimeout(), ElectionMaxTimeout()
//       * internal/replication/manager.go → ReplicationTimeout()
// =============================================================================
package config

import "time"

// Config holds all configuration parameters for a node.
type Config struct {
	NodeID               string   `json:"node_id"`               // unique identifier (e.g., "node1")
	Port                 int      `json:"port"`                  // TCP port for gRPC server
	Peers                []string `json:"peers"`                 // addresses of peer nodes
	HeartbeatMs          int      `json:"heartbeat_ms"`          // heartbeat interval in milliseconds
	ElectionMinMs        int      `json:"election_min_ms"`       // minimum election timeout in ms
	ElectionMaxMs        int      `json:"election_max_ms"`       // maximum election timeout in ms
	ReplicationTimeoutMs int      `json:"replication_timeout_ms"` // replication timeout in ms
}

// Default returns a Config with sensible default values for local development.
//
// TODO: Return a Config with:
//   NodeID="node1", Port=5001, Peers=[], HeartbeatMs=150,
//   ElectionMinMs=300, ElectionMaxMs=500, ReplicationTimeoutMs=1000
func Default() Config {
	return Config{}
}

// HeartbeatInterval returns the heartbeat interval as a time.Duration.
// The leader sends heartbeats at this interval to prevent elections.
//
// TODO: Convert HeartbeatMs to time.Duration.
func (c Config) HeartbeatInterval() time.Duration {
	return 0
}

// HeartbeatTimeout returns the timeout after which a leader is considered failed.
// Set to 2x the heartbeat interval to tolerate one missed heartbeat.
//
// TODO: Return 2 * HeartbeatInterval().
func (c Config) HeartbeatTimeout() time.Duration {
	return 0
}

// ElectionMinTimeout returns the minimum election timeout as a Duration.
//
// TODO: Convert ElectionMinMs to time.Duration.
func (c Config) ElectionMinTimeout() time.Duration {
	return 0
}

// ElectionMaxTimeout returns the maximum election timeout as a Duration.
//
// TODO: Convert ElectionMaxMs to time.Duration.
func (c Config) ElectionMaxTimeout() time.Duration {
	return 0
}

// ReplicationTimeout returns the replication timeout as a Duration.
// If quorum is not reached within this timeout, the publish fails.
//
// TODO: Convert ReplicationTimeoutMs to time.Duration.
func (c Config) ReplicationTimeout() time.Duration {
	return 0
}

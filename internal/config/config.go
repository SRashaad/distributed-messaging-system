// Module: Config
// Phase: Foundation
// Purpose: Defines minimal node configuration and loading hooks for startup.
// Extended in later phases by timeout tuning, validation, and dynamic config sources.
package config

import (
	"flag"
	"strings"
	"time"
)

// Config holds the minimum required node settings.
type Config struct {
	NodeID           string
	Port             int
	Peers            []string
	ZookeeperServers []string
	HeartbeatMs      int
	ElectionMinMs    int
	ElectionMaxMs    int
}

// Load returns startup configuration for the current node process.
// It reads values from command-line flags.
func Load() (Config, error) {
	nodeID := flag.String("id", "node-1", "unique node ID")
	port := flag.Int("port", 5001, "gRPC listen port")
	peers := flag.String("peers", "", "comma-separated peer addresses (e.g., localhost:5002,localhost:5003)")
	zkServers := flag.String("zk", "localhost:2181", "comma-separated ZooKeeper addresses")
	heartbeatMs := flag.Int("heartbeat-ms", 150, "heartbeat interval in milliseconds")
	electionMinMs := flag.Int("election-min-ms", 300, "minimum election timeout in ms")
	electionMaxMs := flag.Int("election-max-ms", 500, "maximum election timeout in ms")

	flag.Parse()

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	var zkList []string
	if *zkServers != "" {
		zkList = strings.Split(*zkServers, ",")
	} else {
		zkList = []string{"localhost:2181"}
	}

	return Config{
		NodeID:           *nodeID,
		Port:             *port,
		Peers:            peerList,
		ZookeeperServers: zkList,
		HeartbeatMs:      *heartbeatMs,
		ElectionMinMs:    *electionMinMs,
		ElectionMaxMs:    *electionMaxMs,
	}, nil
}

// HeartbeatInterval returns the heartbeat interval as a time.Duration.
func (c Config) HeartbeatInterval() time.Duration {
	return time.Duration(c.HeartbeatMs) * time.Millisecond
}

// HeartbeatTimeout returns the timeout (2x heartbeat interval).
func (c Config) HeartbeatTimeout() time.Duration {
	return 2 * c.HeartbeatInterval()
}

// ElectionMinTimeout returns the minimum election timeout.
func (c Config) ElectionMinTimeout() time.Duration {
	return time.Duration(c.ElectionMinMs) * time.Millisecond
}

// ElectionMaxTimeout returns the maximum election timeout.
func (c Config) ElectionMaxTimeout() time.Duration {
	return time.Duration(c.ElectionMaxMs) * time.Millisecond
}

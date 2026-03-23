// Module: Config
// Phase: Foundation
// Purpose: Defines minimal node configuration and loading hooks for startup.
// Extended in later phases by timeout tuning, validation, and dynamic config sources.
package config

// Config holds the minimum required node settings.
type Config struct {
	NodeID string
	Port   int
	Peers  []string
}

// Load returns startup configuration for the current node process.
// TODO: Read values from flags, env, or config file.
func Load() (Config, error) {
	return Config{
		NodeID: "node-1",
		Port:   5001,
		Peers:  []string{},
	}, nil
}

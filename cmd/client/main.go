// =============================================================================
// Entry Point: Client
// File: cmd/client/main.go
// Responsible Member: All Members (Shared)
// Purpose: A simple CLI client for interacting with the distributed messaging
//          cluster. It connects to a node via gRPC and supports two operations:
//            - publish: send a new message to the cluster
//            - consume: retrieve committed messages from the cluster
//
// Usage:
//   go run cmd/client/main.go --leader "localhost:5001" --action publish --message "Hello"
//   go run cmd/client/main.go --leader "localhost:5001" --action consume
// =============================================================================
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	leader := flag.String("leader", "localhost:5001", "address of the leader node")
	action := flag.String("action", "", "action to perform: publish or consume")
	message := flag.String("message", "", "message to publish (required for publish action)")

	flag.Parse()

	if *action == "" {
		fmt.Fprintln(os.Stderr, "Usage: client --leader <addr> --action <publish|consume> [--message <msg>]")
		os.Exit(1)
	}

	switch *action {
	case "publish":
		if *message == "" {
			fmt.Fprintln(os.Stderr, "Error: --message is required for publish action")
			os.Exit(1)
		}
		fmt.Printf("Publishing to %s: %q\n", *leader, *message)
		fmt.Println("(gRPC client integration pending — message queued for consensus)")

		// In full integration:
		// 1. Dial the leader via gRPC
		// 2. Create a MessagingService client
		// 3. Send PublishRequest{Message: []byte(*message)}
		// 4. Print the response (index, timestamp, success)

	case "consume":
		fmt.Printf("Consuming from %s\n", *leader)
		fmt.Println("(gRPC client integration pending — will retrieve committed messages)")

		// In full integration:
		// 1. Dial the leader via gRPC
		// 2. Create a MessagingService client
		// 3. Send ConsumeRequest{FromIndex: 1}
		// 4. Print all returned messages with their Lamport timestamps

	default:
		fmt.Fprintf(os.Stderr, "Unknown action: %q. Use 'publish' or 'consume'.\n", *action)
		os.Exit(1)
	}
}

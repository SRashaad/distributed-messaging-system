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
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"distributed-messaging-system/internal/transport/proto"
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

	// 1. Dial the leader via gRPC
	conn, err := grpc.Dial(*leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to leader %s: %v", *leader, err)
	}
	defer conn.Close()

	// 2. Create a MessagingService client
	client := proto.NewMessagingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *action {
	case "publish":
		if *message == "" {
			fmt.Fprintln(os.Stderr, "Error: --message is required for publish action")
			os.Exit(1)
		}
		fmt.Printf("Publishing to %s: %q\n", *leader, *message)

		// 3. Send PublishRequest
		res, err := client.Publish(ctx, &proto.PublishRequest{
			Message: []byte(*message),
		})
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}

		// 4. Print the response
		fmt.Printf("Success! Message committed at Index: %d, Lamport Timestamp: %d\n", res.Index, res.Timestamp)

	case "consume":
		fmt.Printf("Consuming from %s\n", *leader)

		// 3. Send ConsumeRequest
		res, err := client.Consume(ctx, &proto.ConsumeRequest{
			FromIndex: 1,
		})
		if err != nil {
			log.Fatalf("Consume failed: %v", err)
		}

		// 4. Print all returned messages
		if len(res.Messages) == 0 {
			fmt.Println("No messages found in the log.")
		} else {
			fmt.Println("Committed Messages:")
			for _, msg := range res.Messages {
				fmt.Printf("  - Index: %d | Term: %d | Time: %d | Data: %s\n", msg.Index, msg.Term, msg.Timestamp, string(msg.Data))
			}
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown action: %q. Use 'publish' or 'consume'.\n", *action)
		os.Exit(1)
	}
}


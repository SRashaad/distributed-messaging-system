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
		
		var res *proto.PublishResponse
		var err error
		currentLeader := *leader
		maxRetries := 3

		for i := 0; i < maxRetries; i++ {
			fmt.Printf("Publishing to %s: %q\n", currentLeader, *message)
			
			// Dial the current leader
			conn, dErr := grpc.Dial(currentLeader, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if dErr != nil {
				log.Fatalf("Failed to connect to leader %s: %v", currentLeader, dErr)
			}
			msgClient := proto.NewMessagingServiceClient(conn)

			res, err = msgClient.Publish(ctx, &proto.PublishRequest{
				Message: []byte(*message),
			})

			if err != nil {
				errMsg := err.Error()

				// 1. Hard Crash Redirect (TCP Connection Refused)
				if len(errMsg) > 0 && (errMsg[len(errMsg)-11:] == "refused it." || errMsg[27:38] == "Unavailable" || currentLeader != "undefined") {
					// We'll use a safer robust check for strings
					importNeeded := false
					for _, b := range errMsg {
						if b == 'U' { importNeeded = true }
					}
					_ = importNeeded
				}
				
				// Using simple string matching logic without importing strings
				isUnreachable := false
				if len(errMsg) > 10 {
					for x := 0; x < len(errMsg)-10; x++ {
						if errMsg[x:x+11] == "Unavailable" || errMsg[x:x+7] == "refused" {
							isUnreachable = true
							break
						}
					}
				}

				if isUnreachable {
					fmt.Printf("Node %s is unreachable. Actively failing over...\n", currentLeader)
					conn.Close()
					if currentLeader == "localhost:5001" {
						currentLeader = "localhost:5002"
					} else if currentLeader == "localhost:5002" {
						currentLeader = "localhost:5003"
					} else {
						currentLeader = "localhost:5001"
					}
					continue
				}

				// 2. Soft Redirect (Follower Proxying)
				if len(errMsg) > 5 {
					if errMsg[len(errMsg)-5:] == "node1" {
						currentLeader = "localhost:5001"
						conn.Close()
						fmt.Printf("Redirecting to true leader: %s\n", currentLeader)
						continue
					} else if errMsg[len(errMsg)-5:] == "node2" {
						currentLeader = "localhost:5002"
						conn.Close()
						fmt.Printf("Redirecting to true leader: %s\n", currentLeader)
						continue
					} else if errMsg[len(errMsg)-5:] == "node3" {
						currentLeader = "localhost:5003"
						conn.Close()
						fmt.Printf("Redirecting to true leader: %s\n", currentLeader)
						continue
					}
				}
				
				conn.Close()
				log.Fatalf("Publish failed: %v", err)
			}

			conn.Close()
			break // Success
		}
		
		if err != nil {
			log.Fatalf("Publish failed after retries: %v", err)
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


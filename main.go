package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
)

// DebeziumPayload represents the structure of the JSON message from Debezium.
type DebeziumPayload struct {
	Payload struct {
		After struct {
			UserID    int    `json:"user_id"`
			StockID   int    `json:"stock_id"`
			TradeType string `json:"trade_type"`
			Quantity  int    `json:"quantity_traded"`
		} `json:"after"`
	} `json:"payload"`
}

// WebSocketMessage is the structure of the message we'll send to the WebSocket hub.
type WebSocketMessage struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "trading-feed-consumer",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// Subscribe to the topic where Debezium publishes transaction changes.
	consumer.SubscribeTopics([]string{"tradingplatform.public.orders"}, nil)

	// Handle graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var msg DebeziumPayload
				if err := json.Unmarshal(e.Value, &msg); err != nil {
					log.Printf("Error unmarshalling Debezium message: %v\n", err)
					continue
				}

				// We only care about new inserts ('c' for create).
				// For a full system you might handle updates ('u') and deletes ('d') as well.
				// Debezium's 'op' field is not directly in this simplified struct, but you'd check it.

				// Create a user-friendly message.
				// In a real app, you'd look up user/stock names from IDs.
				feedMessage := fmt.Sprintf("A user just %s %d shares of stock ID %d!",
					msg.Payload.After.TradeType,
					msg.Payload.After.Quantity,
					msg.Payload.After.StockID,
				)

				log.Printf("Processed trade: %s\n", feedMessage)

				// Push this message to the main WebSocket hub.
				pushToWebSocket(feedMessage)

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}
}

// pushToWebSocket sends the formatted message to a new endpoint on your main Go backend.
func pushToWebSocket(message string) {
	// The message to be sent to the WebSocket hub

	_ = godotenv.Load()
	mainGoBackendUrl := os.Getenv("MAIN_GO_BACKEND_URL")

	wsMsg := WebSocketMessage{
		Type:    "TRADING_FEED_UPDATE",
		Content: message,
	}

	jsonData, err := json.Marshal(wsMsg)
	if err != nil {
		log.Printf("Error marshalling WebSocket message: %v\n", err)
		return
	}

	// This assumes your main Go backend has a new, internal-only endpoint to receive these messages.
	resp, err := http.Post(mainGoBackendUrl+"/internal/broadcast", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error pushing to WebSocket hub: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("WebSocket hub returned non-OK status: %s\n", resp.Status)
	}
}

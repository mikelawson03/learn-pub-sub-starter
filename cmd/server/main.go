package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connURL := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connURL)
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ: ", err)
		return
	}
	defer conn.Close()
	fmt.Println("Successfully connected to Peril server at: ", connURL)

	pubChannel, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating new pub channel: ", err)
		return
	}

	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, "game_logs", "game_logs.*", pubsub.SimpleQueueDurable, handlerLogs())
	if err != nil {
		log.Fatalf("could not subscribe to queue: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch strings.ToLower(input[0]) {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(pubChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Println("Error publishing JSON: ", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(pubChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Println("Error publishing JSON: ", err)
			}
		case "quit":
			fmt.Println("Exiting")
			return
		default:
			fmt.Println("Unknown command. Please try again.")
		}

	}
}

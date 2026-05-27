package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connURL := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connURL)
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
	}

	defer conn.Close()
	fmt.Println("Successfully connected to Peril server at: ", connURL)

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error: ", err)
	}

	state := gamelogic.NewGameState(username)

	// subscribe to Pause Queue
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("%v.%v", routing.PauseKey, username), routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(state))
	if err != nil {
		log.Fatalf("Error subscribing to queue: %v", err)
	}

	pubChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error creating pub channel: %v", err)
	}

	// subscribe to Move Queue
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%v", username), "army_moves.*", pubsub.SimpleQueueTransient, handlerMove(state, pubChannel))
	if err != nil {
		log.Fatalf("Error subscribing to queue: %v", err)
	}

	// subscribe to War Queue
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), pubsub.SimpleQueueDurable, handlerConsumeWarMessage(state, pubChannel))

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch strings.ToLower(input[0]) {
		case "spawn":
			if err := state.CommandSpawn(input); err != nil {
				fmt.Println(err)
			}
		case "move":
			move, err := state.CommandMove(input)
			if err != nil {
				fmt.Println(err)
			}

			err = pubsub.PublishJSON(pubChannel, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%v", username), move)
			if err != nil {
				log.Fatalf("Error publishing move: %v", err)
			}
			fmt.Println("Move successful")

		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 {
				fmt.Println("Please enter a value of to spam (e.g. spam 1000)")
				continue
			}
			spamCount, err := strconv.Atoi(input[1])
			if err != nil {
				log.Fatalf("Error converting spam count to integer: %v", err)
			}
			for i := 0; i <= spamCount; i += 1 {
				msg := gamelogic.GetMaliciousLog()
				gl := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     msg,
					Username:    username,
				}
				pubsub.PublishGob(pubChannel, routing.ExchangePerilTopic, fmt.Sprintf("game_logs.%s", username), gl)
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Please try again.")
		}

	}
}

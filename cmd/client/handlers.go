package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.AckTypeAck
		case gamelogic.MoveOutComeSafe:
			return pubsub.AckTypeAck
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%v.%v",
					routing.WarRecognitionsPrefix,
					gs.Player.Username),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				return pubsub.AckTypeRequeue
			}
			return pubsub.AckTypeAck
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.AckTypeDiscard
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.AckTypeAck
	}
}

func handlerConsumeWarMessage(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.AckTypeRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.AckTypeDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(gs, ch, msg)
			if err != nil {
				fmt.Printf("Failed to publish message: %v\n", err)
				return pubsub.AckTypeRequeue
			}
			return pubsub.AckTypeAck
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(gs, ch, msg)
			if err != nil {
				fmt.Printf("Failed to publish message: %v\n", err)
				return pubsub.AckTypeRequeue
			}
			return pubsub.AckTypeAck
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(gs, ch, msg)
			if err != nil {
				fmt.Printf("Failed to publish message: %v\n", err)
				return pubsub.AckTypeRequeue
			}
			return pubsub.AckTypeAck
		default:
			fmt.Println("Unknown war message received. Discarding...")
			return pubsub.AckTypeDiscard
		}
	}
}

func publishGameLog(gs *gamelogic.GameState, ch *amqp.Channel, msg string) error {
	gl := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    gs.GetUsername(),
	}

	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.GetUsername()), gl)
	if err != nil {
		return err
	}

	return nil
}

package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	AckTypeAck AckType = iota
	AckTypeRequeue
	AckTypeDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	res, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Error marshaling JSON: %v", err)
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application.json",
		Body:        res,
	})

	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	newChan, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	dur := true
	autodel := false
	excl := false

	if queueType == SimpleQueueTransient {
		dur = false
		autodel = true
		excl = true
	}

	newQueue, err := newChan.QueueDeclare(queueName, dur, autodel, excl, false, amqp.Table{
		"x-dead-letter-exchange": routing.ExchangePerilDeadLetter,
	},
	)
	if err != nil {
		return newChan, amqp.Queue{}, err
	}

	if err = newChan.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return newChan, newQueue, err
	}

	return newChan, newQueue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		log.Fatalf("could not subscribe to %v: %v", exchange, err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	dChan, err := ch.Consume(queue.Name, "", false, false, false, false, nil)

	go func() {
		for d := range dChan {
			var payload T
			err := json.Unmarshal(d.Body, &payload)
			if err != nil {
				log.Fatalf("Error unmarshaling JSON: %v", err)
			}
			ack := handler(payload)
			switch ack {
			case AckTypeAck:
				d.Ack(false)
				log.Printf("message acknowledged")
			case AckTypeRequeue:
				d.Nack(false, true)
				log.Printf("message nacked. requeueing")
			case AckTypeDiscard:
				d.Nack(false, false)
				log.Printf("message nacked. discarding.")
			}

		}
	}()

	return nil
}

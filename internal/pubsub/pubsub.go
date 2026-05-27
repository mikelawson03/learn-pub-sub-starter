package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(val)
	if err != nil {
		return fmt.Errorf("Error encoding message: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	})
	if err != nil {
		return fmt.Errorf("Error publishing message: %v", err)
	}

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
	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {

	return subscribe[T](
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			buff := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buff)
			var target T
			err := dec.Decode(&target)
			return target, err
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not subscribe to %v: %v", exchange, err)
	}

	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("Error decoding message: %v", err)
				continue
			}
			ack := handler(target)
			switch ack {
			case AckTypeAck:
				msg.Ack(false)
			case AckTypeRequeue:
				msg.Nack(false, true)
			case AckTypeDiscard:
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}

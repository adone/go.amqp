package amqp

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"gopkg.in/adone/go.events.v2"
)

// Consumer consume messages from message broker
type Consumer struct {
	*events.Emitter

	MaxMessages int
	Queue       string

	tag     string
	channel *amqp.Channel
	error   chan error
	done    chan struct{}
}

// Start consuming messages
func (consumer *Consumer) Start() error {
	consumer.done = make(chan struct{})
	consumer.error = make(chan error)

	defer close(consumer.error)

	if err := consumer.channel.Qos(consumer.MaxMessages, 0, false); err != nil {
		return err
	}

	deliveries, err := consumer.channel.Consume(
		consumer.Queue, // name
		consumer.tag,   // consumerTag
		false,          // noAck
		false,          // exclusive
		false,          // noLocal
		false,          // noWait
		nil,            // arguments
	)

	if err != nil {
		return err
	}

	go consumer.listenChannel()
	go consumer.listenDeliveries(deliveries)

	return <-consumer.error
}

// Stop consumimg messages
func (consumer *Consumer) Stop() error {
	select {
	case <-consumer.done:
		return nil
	default:
		// manual call to channel.Cancel does not fire NotifyCancel, it just close deliveries chan
		if err := consumer.channel.Cancel(consumer.tag, false); err != nil {
			return errors.Wrap(err, "channel cancel failed")
		}

		return nil
	}
}

func (consumer *Consumer) Close() error {
	return consumer.channel.Close()
}

func (consumer *Consumer) listenDeliveries(deliveries <-chan amqp.Delivery) {
	defer close(consumer.done)

	for delivery := range deliveries {
		consumer.Fire(events.New(ConsumerData, events.WithContext(events.Map{
			"key":   delivery.RoutingKey,
			"data":  Message{delivery},
			"queue": consumer.Queue,
		})))
	}
}

func (consumer *Consumer) listenChannel() {
	select {
	case reason := <-consumer.channel.NotifyCancel(make(chan string)):
		consumer.Fire(events.New(ConsumerCanceled, events.WithContext(events.Map{"consumer": consumer})))
		consumer.error <- errors.Errorf("channel canceled: %s", reason)
	case err := <-consumer.channel.NotifyClose(make(chan *amqp.Error)):
		consumer.Fire(events.New(ConsumerClosed, events.WithContext(events.Map{"consumer": consumer, "error": err})))
		consumer.error <- err
	case <-consumer.done:
		consumer.error <- nil
	}
}

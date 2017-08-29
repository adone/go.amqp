package amqp

import (
	"github.com/streadway/amqp"
)

type Message struct {
	amqp.Delivery
}

func (message Message) Key() string {
	return message.RoutingKey
}

func (message Message) Content() []byte {
	return message.Body
}

func (message Message) Confirm() {
	message.Ack(false)
}

func (message Message) Reject() {
	message.Nack(false, true)
}

package amqp

import (
	"github.com/streadway/amqp"

	"strconv"
)

type Producer struct {
	counter  uint64
	Exchange string
	confirms chan amqp.Confirmation
	channel  *amqp.Channel
}

type Confirmation struct {
}

func (producer *Producer) Publish(key string, body []byte, options ...PublishOption) error {
	po := publishOptions{}
	for _, option := range options {
		option.f(&po)
	}

	publishig := amqp.Publishing{
		Headers:         amqp.Table{"x-retry-count": po.retryCount},
		ContentType:     "application/json",
		ContentEncoding: "",
		Body:            body,
		DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
		Priority:        0,               // 0-9
	}

	if po.messageTTL > 0 {
		publishig.Expiration = strconv.FormatInt(po.messageTTL, 10)
	}

	return producer.channel.Publish(
		producer.Exchange, // publish to an exchange
		key,               // routing to 0 or more queues
		false,             // mandatory
		false,             // immediate
		publishig,
	)
}

// func (producer *Producer) start() {
// 	for confirmation := range producer.confirms {
// 		fmt.Println("CONFIRMATION", confirmation)
// 	}
// }

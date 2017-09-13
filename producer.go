package amqp

import (
	"github.com/streadway/amqp"

	"strconv"
)

// PublishOption опции, передаваемые при публикации сообщения
type PublishOption struct {
	apply func(*publishParams)
}

type publishParams struct {
	messageTTL int64
	retryCount int64
}

// WithMessageTTL sets TTL to message
// parameter count in milliseconds
func WithMessageTTL(ttl int64) PublishOption {
	return PublishOption{func(params *publishParams) {
		params.messageTTL = ttl
	}}
}

// WithRetryCount sets max number of retries
func WithRetryCount(count int64) PublishOption {
	return PublishOption{func(params *publishParams) {
		params.retryCount = count
	}}
}

type Producer struct {
	counter  uint64
	exchange Exchange
	channel  *amqp.Channel
}

func (producer *Producer) Publish(key string, payload []byte, options ...PublishOption) error {
	params := publishParams{}

	for _, option := range options {
		option.apply(&params)
	}

	publishig := amqp.Publishing{
		Headers:         amqp.Table{"x-retry-count": params.retryCount},
		ContentType:     "application/json",
		ContentEncoding: "",
		Body:            payload,
		DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
		Priority:        0,               // 0-9
	}

	if params.messageTTL > 0 {
		publishig.Expiration = strconv.FormatInt(params.messageTTL, 10)
	}

	return producer.channel.Publish(
		producer.exchange.Name, // publish to an exchange
		key,   // routing to 0 or more queues
		false, // mandatory
		false, // immediate
		publishig,
	)
}

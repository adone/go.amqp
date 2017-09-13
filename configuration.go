package amqp

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Binding struct {
	Exchange string `yaml:"exchange"`
	Key      string `yaml:"key"`
}

type Exchange struct {
	Name     string    `yaml:"name"`
	Type     string    `yaml:"type"`
	Durable  bool      `yaml:"durable"`
	Bindings []Binding `yaml:"bindings"`
}

func (exchange Exchange) declare(channel *amqp.Channel) error {
	err := channel.ExchangeDeclare(
		exchange.Name,    // name of the exchange
		exchange.Type,    // type
		exchange.Durable, // durable
		false,            // delete when complete
		false,            // internal
		false,            // noWait
		nil,              // arguments
	)

	if err != nil {
		return errors.Wrap(err, "declare exchange failed")
	}

	if len(exchange.Bindings) > 0 {
		for _, binding := range exchange.Bindings {
			err = channel.ExchangeBind(
				exchange.Name,    // name of the exchange
				binding.Key,      // bindingKey
				binding.Exchange, // sourceExchange
				false,            // noWait
				nil,              // arguments
			)

			if err != nil {
				return errors.Wrap(err, "bind exchange failed")
			}
		}
	}

	return nil
}

type Queue struct {
	Name      string    `yaml:"name"`
	Key       string    `yaml:"key"`
	Durable   bool      `yaml:"durable"`
	Deletable bool      `yaml:"deletable"`
	Bindings  []Binding `yaml:"bindings"`
	Arguments Arguments `yaml:"arguments"`
}

func (queue Queue) declare(channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(
		queue.Name,      // name of the queue
		queue.Durable,   // durable
		queue.Deletable, // delete when usused
		false,           // exclusive
		false,           // noWait
		queue.Arguments.ToMap(), // arguments
	)

	if err != nil {
		return errors.Wrap(err, "declare queue failed")
	}

	if len(queue.Bindings) > 0 {
		for _, binding := range queue.Bindings {
			err = channel.QueueBind(
				queue.Name,       // name of the queue
				binding.Key,      // bindingKey
				binding.Exchange, // sourceExchange
				false,            // noWait
				nil,              // arguments
			)

			if err != nil {
				return errors.Wrap(err, "bind queue failed")
			}
		}
	}

	return nil
}

type Arguments struct {
	MessageTTL         int64  `yaml:"x-message-ttl"`
	DeadLetterExchange string `yaml:"x-dead-letter-exchange"`
}

func (args Arguments) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	if args.MessageTTL > 0 {
		result["x-message-ttl"] = args.MessageTTL
	}

	if args.DeadLetterExchange != "" {
		result["x-dead-letter-exchange"] = args.DeadLetterExchange
	}

	return result
}

type Configuration struct {
	URL       string     `yaml:"url"`
	Node      string     `yaml:"node"`
	Exchanges []Exchange `yaml:"exchanges"`
	Queues    []Queue    `yaml:"queues"`
}

func (config *Configuration) GetNodeName() string {
	if config.Node == "" {
		return "node"
	}

	return config.Node
}

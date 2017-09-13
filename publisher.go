package amqp

import (
	"fmt"
	"sync"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"gopkg.in/adone/go.events.v2"
)

type state int

const (
	created state = iota
	connecting
	connected
	failed
)

type Publisher struct {
	sync.Mutex
	*events.Emitter

	params   PublisherParams
	driver   *Driver
	producer *Producer
	state    state
	err      error
	buffer   map[*bufferedMessage]string
}

type bufferedMessage struct {
	options []PublishOption
	payload []byte
}

func NewPublisher(driver *Driver, params PublisherParams) *Publisher {
	publisher := new(Publisher)
	publisher.Emitter = events.NewEmitter(events.WithEventStategy(PublishError, events.ParallelBroadcast))
	publisher.params = params
	publisher.driver = driver
	publisher.buffer = make(map[*bufferedMessage]string)

	publisher.On(ProducerConnected, events.Callback(func(event events.Event) {
		publisher.Lock()
		defer publisher.Unlock()

		for message, key := range publisher.buffer {
			if err := publisher.producer.Publish(key, message.payload, message.options...); err == nil {
				delete(publisher.buffer, message)
			} else {
				publisher.state = connecting

				go publisher.connect()

				publisher.Fire(events.New(PublishError, events.WithContext(events.Map{"error": err.Error()})))

				break
			}
		}
	}))

	return publisher
}

func (publisher *Publisher) Publish(key string, payload []byte, options ...PublishOption) error {
	publisher.Lock()
	defer publisher.Unlock()

	message := &bufferedMessage{options, payload}

	switch publisher.state {
	case created:
		publisher.state = connecting

		go publisher.connect()

		publisher.buffer[message] = key
	case connecting:
		publisher.buffer[message] = key
	case connected:
		if err := publisher.producer.Publish(key, payload, options...); err != nil {
			publisher.state = connecting

			go publisher.connect()

			publisher.Fire(events.New(PublishError, events.WithContext(events.Map{"error": err.Error()})))

			publisher.buffer[message] = key
		}
	case failed:
		return publisher.err
	}

	return nil // for backward with Producer interface
}

func (publisher *Publisher) connect() {
	var producer *Producer

	connect := func(attempt uint) (err error) {
		producer, err = publisher.driver.Producer(publisher.params.Exchange)
		return
	}

	if err := retry.Retry(connect, strategy.Limit(publisher.params.Retries), strategy.Wait(publisher.params.Pause)); err != nil {
		publisher.Lock()
		publisher.state = failed
		publisher.err = err
		publisher.Unlock()

		publisher.Fire(events.New(PublisherFailed, events.WithContext(events.Map{
			"error": fmt.Sprintf("connect to AMQP failed after %d retries: %s", publisher.params.Retries, err),
		})))

		return
	}

	publisher.Lock()
	publisher.state = connected
	publisher.producer = producer
	publisher.Unlock()

	publisher.Fire(events.New(ProducerConnected, events.WithContext(events.Map{"producer": producer})))
}

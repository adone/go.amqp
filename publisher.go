package amqp

import (
	"fmt"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"gopkg.in/adone/go.events.v2"
)

type State int

const (
	Created State = iota
	Connecting
	Connected
	Failed
)

type Publisher struct {
	sync.Mutex
	*events.Emitter
	RetryPause time.Duration
	MaxRetries int

	driver   *Driver
	producer *Producer
	state    State
	err      error
	buffer   map[*publishMsg]string
}

type publishMsg struct {
	opts []PublishOption
	body []byte
}

// PublishOption опции, передаваемые при публикации сообщения
type PublishOption struct {
	f func(*publishOptions)
}

type publishOptions struct {
	messageTTL int64
	retryCount int64
}

// WithMessageTTL установка ttl сообщения
// ttl в милисекундах
// WARNING: сообщения из очереди удаляются по принципу last-out,
// т.е. если перед сообщением с небольшим ttl находится сообщение с
// большим ttl, сообщение с меньшим ttl не будет удалено из очереди, пока
// не будет удалено сообщение с большим ttl
func WithMessageTTL(ttl int64) PublishOption {
	return PublishOption{func(po *publishOptions) {
		po.messageTTL = ttl
	}}
}

// WithRetryCount фиксирование номера попытки
// Число сохраняется в отдельный хедер "x-retry-count"
func WithRetryCount(retry int64) PublishOption {
	return PublishOption{func(po *publishOptions) {
		po.retryCount = retry
	}}
}

func NewPublisher(driver *Driver) *Publisher {
	publisher := new(Publisher)
	publisher.Emitter = events.NewEmitter()
	publisher.driver = driver
	publisher.MaxRetries = 3
	publisher.RetryPause = 1 * time.Second
	publisher.buffer = make(map[*publishMsg]string)

	publisher.On("flush", events.Callback(func(event events.Event) {
		publisher.Lock()
		defer publisher.Unlock()

		for message, key := range publisher.buffer {
			if err := publisher.producer.Publish(key, message.body, message.opts...); err == nil {
				delete(publisher.buffer, message)
			} else {
				publisher.state = Connecting
				go publisher.connect()
				go publisher.Fire(events.New("failed", events.WithContext(events.Map{"message": err.Error()})))

				break
			}
		}
	}))

	return publisher
}

func (publisher *Publisher) Publish(key string, body []byte, options ...PublishOption) error {
	publisher.Lock()
	defer publisher.Unlock()

	msg := &publishMsg{
		body: body,
		opts: options,
	}

	switch publisher.state {
	case Created:
		publisher.state = Connecting
		go publisher.connect()
		publisher.buffer[msg] = key
	case Connecting:
		publisher.buffer[msg] = key
	case Connected:
		if err := publisher.producer.Publish(key, body, options...); err != nil {
			publisher.state = Connecting
			go publisher.connect()
			go publisher.Fire(events.New("failed", events.WithContext(events.Map{"message": err.Error()})))
			publisher.buffer[msg] = key
		}
	case Failed:
		return publisher.err
	}

	return nil // for backward with Producer interface
}

func (publisher *Publisher) connect() {
	var (
		producer *Producer
		err      error
	)

	connect := func(attempt uint) error {
		if producer, err = publisher.driver.Producer(); err != nil {
			return err
		}

		return nil
	}

	if err := retry.Retry(
		connect,
		// strategy.Limit(publisher.MaxRetries),
		strategy.Wait(publisher.RetryPause),
	); err != nil {
		publisher.Fire(events.New("error", events.WithContext(events.Map{"message": fmt.Sprintf("Publisher: producer: %s", err)})))
		publisher.err = err
		return
	}

	publisher.Lock()
	publisher.state = Connected
	publisher.Fire(events.New("connect", events.WithContext(events.Map{"producer": producer})))
	publisher.producer = producer
	publisher.Unlock()

	publisher.Fire("flush")
}

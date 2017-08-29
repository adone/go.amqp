package amqp

import (
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"
	"gopkg.in/adone/go.events.v2"
)

type params struct {
	event    string
	queue    Queue
	retries  uint
	pause    time.Duration
	failfast bool
	qos      int
}

var defaults = params{
	retries: 10,
	pause:   time.Second,
}

type ListenOption struct {
	apply func(*params)
}

func WithQueue(queue Queue) ListenOption {
	return ListenOption{
		apply: func(params *params) {
			params.queue = queue
		},
	}
}

func WithEvent(event string) ListenOption {
	return ListenOption{
		apply: func(params *params) {
			params.event = event
		},
	}
}

func WithQoS(count int) ListenOption {
	return ListenOption{
		apply: func(params *params) {
			params.qos = count
		},
	}
}

func WithMaxRetries(count uint) ListenOption {
	return ListenOption{
		apply: func(params *params) {
			params.retries = count
		},
	}
}

func WithRetryPause(duration time.Duration) ListenOption {
	return ListenOption{
		apply: func(params *params) {
			params.pause = duration
		},
	}
}

type Listener struct {
	*events.Emitter

	guard    sync.Mutex
	start    chan events.Event
	stop     chan events.Event
	errors   chan events.Event
	data     chan events.Event
	driver   *Driver
	consumer *Consumer
}

func NewListener(driver *Driver) *Listener {
	listener := new(Listener)
	listener.Emitter = events.NewEmitter()

	listener.driver = driver

	listener.start = make(chan events.Event, 1)
	listener.stop = make(chan events.Event, 1)
	listener.data = make(chan events.Event)
	listener.errors = make(chan events.Event)

	return listener
}

func (listener *Listener) Listen(options ...ListenOption) error {
	params := defaults

	for _, option := range options {
		option.apply(&params)
	}

	if params.queue.Name == "" {
		return errors.Errorf("empty queue name")
	}

	listener.
		On(ListenStart, events.Stream(listener.start)).
		On(ListenStop, events.Stream(listener.stop)).
		On(ListenError, events.Stream(listener.errors))

	defer listener.RemoveEventListener(events.Stream(listener.start))
	defer listener.RemoveEventListener(events.Stream(listener.stop))
	defer listener.RemoveEventListener(events.Stream(listener.errors))

	consumer, err := listener.restart(nil, params)
	if err != nil {
		return err
	}

	for {
		select {
		case event := <-listener.data:
			if params.event != "" {
				listener.Fire(events.New(params.event, events.WithContext(event.Context)))
			} else {
				listener.Fire(events.New(event.Context["key"], events.WithContext(event.Context)))
			}
		case <-listener.errors:
			consumer, err = listener.restart(consumer, params)
			if err != nil {
				return err
			}
		case <-listener.start:
			go listener.listen(consumer)
		case <-listener.stop:
			return listener.shutdown(consumer)
		}
	}
}

func (listener *Listener) Stop() {
	listener.Fire(ListenStop)
}

func (listener *Listener) listen(consumer *Consumer) {
	if err := consumer.Start(); err != nil {
		listener.Fire(events.New(ListenError, events.WithContext(events.Map{"error": errors.Wrap(err, "consume failed")})))
	}
}

func (listener *Listener) shutdown(consumer *Consumer) error {
	if consumer != nil {
		consumer.RemoveEventListener(events.Stream(listener.start))
		consumer.RemoveEventListener(events.Stream(listener.errors))
		consumer.RemoveEventListener(events.Stream(listener.data))
		if err := consumer.Stop(); err != nil {
			return err
		}

		return consumer.Close()
	}

	return nil
}

func (listener *Listener) restart(consumer *Consumer, params params) (*Consumer, error) {
	listener.shutdown(consumer)

	connect := func(attempt uint) (err error) {
		consumer, err = listener.driver.Consumer()
		return
	}

	if err := retry.Retry(connect, strategy.Limit(params.retries), strategy.Wait(params.pause)); err != nil {
		err = errors.Wrapf(err, "connect to AMQP queue '%s' failed after %d retries", params.queue.Name, params.retries)
		if params.failfast {
			return nil, err
		}

		listener.Fire(events.New(ListenError, events.WithContext(events.Map{"error": err})))
		return nil, nil
	}

	consumer.
		On(ConsumerCanceled, events.Stream(listener.start)).
		On(ConsumerClosed, events.Stream(listener.errors)).
		On(ConsumerData, events.Stream(listener.data))

	consumer.Queue = params.queue.Name
	consumer.MaxMessages = params.qos

	if err := listener.driver.DeclareQueue(params.queue); err != nil {
		return nil, err
	}

	listener.Fire(ListenStart)

	return consumer, nil
}

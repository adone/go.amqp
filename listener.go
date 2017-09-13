package amqp

import (
	"sync"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"
	"gopkg.in/adone/go.events.v2"
)

type Listener struct {
	*events.Emitter

	params ListenerParams
	driver *Driver
	guard  sync.Mutex
	start  chan events.Event
	stop   chan events.Event
	errors chan events.Event
	data   chan events.Event
}

func NewListener(driver *Driver, params ListenerParams) *Listener {
	listener := new(Listener)
	listener.Emitter = events.NewEmitter()
	listener.params = params
	listener.driver = driver
	listener.start = make(chan events.Event, 1)
	listener.stop = make(chan events.Event, 1)
	listener.data = make(chan events.Event)
	listener.errors = make(chan events.Event)

	return listener
}

func (listener *Listener) Listen() error {
	if listener.params.Queue.Name == "" {
		return errors.Errorf("empty queue name")
	}

	listener.
		On(ListenStart, events.Stream(listener.start)).
		On(ListenStop, events.Stream(listener.stop)).
		On(ListenError, events.Stream(listener.errors))

	defer listener.RemoveEventListener(events.Stream(listener.start))
	defer listener.RemoveEventListener(events.Stream(listener.stop))
	defer listener.RemoveEventListener(events.Stream(listener.errors))

	consumer, err := listener.restart(nil)
	if err != nil {
		return err
	}

	for {
		select {
		case event := <-listener.data:
			if listener.params.Event != "" {
				listener.Fire(events.New(listener.params.Event, events.WithContext(event.Context)))
			} else {
				listener.Fire(events.New(event.Context["key"], events.WithContext(event.Context)))
			}
		case <-listener.errors:
			consumer, err = listener.restart(consumer)
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

func (listener *Listener) restart(consumer *Consumer) (*Consumer, error) {
	listener.shutdown(consumer)

	connect := func(attempt uint) (err error) {
		consumer, err = listener.driver.Consumer(listener.params.Queue)
		return
	}

	if err := retry.Retry(connect, strategy.Limit(listener.params.Retries), strategy.Wait(listener.params.Pause)); err != nil {
		err = errors.Wrapf(err, "connect to AMQP queue '%s' failed after %d retries", listener.params.Queue.Name, listener.params.Retries)
		if listener.params.FailFast {
			return nil, err
		}

		listener.Fire(events.New(ListenError, events.WithContext(events.Map{"error": err})))
		return nil, nil
	}

	consumer.
		On(ConsumerCanceled, events.Stream(listener.start)).
		On(ConsumerClosed, events.Stream(listener.errors)).
		On(ConsumerData, events.Stream(listener.data))

	consumer.QoS = listener.params.QoS

	listener.Fire(ListenStart)

	return consumer, nil
}

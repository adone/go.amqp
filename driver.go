package amqp

import (
	"gopkg.in/adone/go.events.v2"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"

	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type ConsumerTagBuilder interface {
	Build(*Driver) string
}

type ConsumerTagBuilderFunc func(*Driver) string

func (builder ConsumerTagBuilderFunc) Build(driver *Driver) string {
	return builder(driver)
}

var (
	DefaultConsumerTagBuilder = ConsumerTagBuilderFunc(func(driver *Driver) string {
		return strings.Join([]string{driver.name, driver.connection.LocalAddr().String(), SecureString(6)}, "-")
	})
)

func New(config *Configuration) (*Driver, error) {
	driver := new(Driver)
	driver.Emitter = events.NewEmitter()
	driver.ConsumerTagBuilder = DefaultConsumerTagBuilder

	driver.name = config.GetNodeName()
	driver.configuration = config
	driver.guard = new(sync.RWMutex)
	driver.lock = sync.NewCond(driver.guard.RLocker())

	return driver, driver.Connect()
}

type Driver struct {
	*events.Emitter
	ConsumerTagBuilder ConsumerTagBuilder

	name          string
	configuration *Configuration
	guard         *sync.RWMutex
	lock          *sync.Cond
	connection    *amqp.Connection
}

func (driver Driver) Name() string {
	return driver.name
}

func (driver *Driver) Connect() error {
	defer driver.lock.Broadcast()

	driver.guard.Lock()
	defer driver.guard.Unlock()

	if err := retry.Retry(driver.connect, strategy.Wait(1*time.Second, 2*time.Second, 5*time.Second)); err != nil {
		return errors.Wrapf(err, "connect to '%s' failed", driver.configuration.URL)
	}

	go driver.watch()

	return driver.setup()
}

func (driver *Driver) connect(attempt uint) (err error) {
	driver.connection, err = amqp.Dial(driver.configuration.URL)
	return
}

func (driver *Driver) watch() {
	error := <-driver.connection.NotifyClose(make(chan *amqp.Error))

	driver.guard.Lock()
	driver.connection = nil
	driver.guard.Unlock()

	driver.Fire(events.New(ConnectionClosed, events.WithContext(events.Map{"error": error})))
}

func (driver *Driver) setup() error {
	channel, err := driver.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "get channel failed")
	}

	defer channel.Close()

	if len(driver.configuration.Exchanges) > 0 {
		for _, exchange := range driver.configuration.Exchanges {
			if err := driver.declareExchange(exchange, channel); err != nil {
				return err
			}
		}
	}

	if len(driver.configuration.Queues) > 0 {
		for _, queue := range driver.configuration.Queues {
			if err := driver.declareQueue(queue, channel); err != nil {
				return err
			}
		}
	}

	return channel.Close()
}

func (driver *Driver) DeclareExchange(exchange Exchange) error {
	driver.guard.RLock()
	defer driver.guard.RUnlock()

	for driver.connection == nil {
		driver.lock.Wait()
	}

	channel, err := driver.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "get channel failed")
	}

	defer channel.Close()

	return driver.declareExchange(exchange, channel)
}

func (Driver) declareExchange(exchange Exchange, channel *amqp.Channel) error {
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

func (driver *Driver) DeclareQueue(queue Queue) error {
	driver.guard.RLock()
	defer driver.guard.RUnlock()

	for driver.connection == nil {
		driver.lock.Wait()
	}

	channel, err := driver.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "get channel failed")
	}

	defer channel.Close()

	return driver.declareQueue(queue, channel)
}

func (Driver) declareQueue(queue Queue, channel *amqp.Channel) error {
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

func (driver *Driver) Producer() (*Producer, error) {
	driver.guard.RLock()
	defer driver.guard.RUnlock()

	for driver.connection == nil {
		driver.lock.Wait()
	}

	channel, err := driver.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "get channel failed")
	}

	if err := channel.Confirm(false); err != nil {
		return nil, errors.Wrap(err, "set confirmation mode failed")
	}

	// producer.counter = 1
	// producer.confirms = producer.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	// go producer.start()
	return &Producer{channel: channel}, nil
}

func (driver *Driver) Consumer() (*Consumer, error) {
	driver.guard.RLock()
	defer driver.guard.RUnlock()

	for driver.connection == nil {
		driver.lock.Wait()
	}

	channel, err := driver.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "get channel failed")
	}

	return &Consumer{
		Emitter: events.NewEmitter(),
		channel: channel,
		tag:     driver.ConsumerTagBuilder.Build(driver),
	}, nil
}

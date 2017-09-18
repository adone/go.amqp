package amqp

import (
	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"gopkg.in/gopaws/go.events.v2"

	"strings"
	"sync"
	"time"
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
			if err := exchange.declare(channel); err != nil {
				return err
			}
		}
	}

	if len(driver.configuration.Queues) > 0 {
		for _, queue := range driver.configuration.Queues {
			if err := queue.declare(channel); err != nil {
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

	return exchange.declare(channel)
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

	return queue.declare(channel)
}

func (driver *Driver) Producer(exchange Exchange) (*Producer, error) {
	driver.guard.RLock()
	defer driver.guard.RUnlock()

	for driver.connection == nil {
		driver.lock.Wait()
	}

	channel, err := driver.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "get channel failed")
	}

	if err := exchange.declare(channel); err != nil {
		return nil, err
	}

	if err := channel.Confirm(false); err != nil {
		return nil, errors.Wrap(err, "set confirmation mode failed")
	}

	return &Producer{
		exchange: exchange,
		channel:  channel,
	}, nil
}

func (driver *Driver) Consumer(queue Queue) (*Consumer, error) {
	driver.guard.RLock()
	defer driver.guard.RUnlock()

	for driver.connection == nil {
		driver.lock.Wait()
	}

	channel, err := driver.connection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "get channel failed")
	}

	if err := queue.declare(channel); err != nil {
		return nil, err
	}

	return &Consumer{
		Emitter: events.NewEmitter(),
		queue:   queue,
		channel: channel,
		tag:     driver.ConsumerTagBuilder.Build(driver),
	}, nil
}

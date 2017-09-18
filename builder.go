package amqp

import (
	"time"
)

func WithConnectRetries(number uint) *Builder {
	return new(Builder).WithConnectRetries(number)
}

func WithConnectPause(pause time.Duration) *Builder {
	return new(Builder).WithConnectPause(pause)
}

func WithQueue(queue Queue) *Builder {
	return new(Builder).WithQueue(queue)
}

func WithExchange(exchange Exchange) *Builder {
	return new(Builder).WithExchange(exchange)
}

func WithEvent(event string) *Builder {
	return new(Builder).WithEvent(event)
}

func WithQoS(number int) *Builder {
	return new(Builder).WithQoS(number)
}

type Builder struct {
	ConnectionParams
	ListenerParams
	PublisherParams
}

func (builder Builder) WithConnectRetries(number uint) *Builder {
	builder.Retries = number
	return &builder
}

func (builder Builder) WithConnectPause(pause time.Duration) *Builder {
	builder.Pause = pause
	return &builder
}

func (builder Builder) WithQueue(queue Queue) *Builder {
	builder.Queue = queue
	return &builder
}

func (builder Builder) WithExchange(exchange Exchange) *Builder {
	builder.Exchange = exchange
	return &builder
}

func (builder Builder) WithEvent(event string) *Builder {
	builder.Event = event
	return &builder
}

func (builder Builder) WithQoS(number int) *Builder {
	builder.QoS = number
	return &builder
}

func (builder Builder) NewListener(driver *Driver) *Listener {
	params := builder.ListenerParams
	params.ConnectionParams = builder.ConnectionParams
	return NewListener(driver, params)
}

func (builder Builder) NewPublisher(driver *Driver) *Publisher {
	params := builder.PublisherParams
	params.ConnectionParams = builder.ConnectionParams
	return NewPublisher(driver, params)
}

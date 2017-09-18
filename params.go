package amqp

import (
	"time"
)

type ConnectionParams struct {
	Pause    time.Duration
	Retries  uint
	FailFast bool
}

type PublisherParams struct {
	ConnectionParams
	Exchange Exchange
}

type ListenerParams struct {
	ConnectionParams
	Event string
	Queue Queue
	QoS   int
}

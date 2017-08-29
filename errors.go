package amqp

type Reconnect struct{}

func (Reconnect) Error() string {
	return "AMQP driver is reconnecting"
}

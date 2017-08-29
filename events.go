package amqp

const (
	ConnectError      = "amqp:connect:error"
	ConnectionClosed  = "amqp:connection:closed"
	ConsumerConnected = "amqp:consumer:connected"
	ConsumerCanceled  = "amqp:consumer:canceled"
	ConsumerClosed    = "amqp:consumer:closed"
	ConsumerData      = "amqp:consumer:data"
	ListenStart       = "amqp:listen:start"
	ListenStop        = "amqp:listen:stop"
	ListenError       = "amqp:listen:error"
)

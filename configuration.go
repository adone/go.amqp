package amqp

type Binding struct {
	Exchange string `yaml:"exchange"`
	Key      string `yaml:"key"`
}

type Exchange struct {
	Name     string    `yaml:"name"`
	Type     string    `yaml:"type"`
	Durable  bool      `yaml:"durable"`
	Bindings []Binding `yaml:"bindings"`
}

type Queue struct {
	Name      string    `yaml:"name"`
	Key       string    `yaml:"key"`
	Durable   bool      `yaml:"durable"`
	Deletable bool      `yaml:"deletable"`
	Bindings  []Binding `yaml:"bindings"`
	Arguments Arguments `yaml:"arguments"`
}

type Arguments struct {
	MessageTTL         int64  `yaml:"x-message-ttl"`
	DeadLetterExchange string `yaml:"x-dead-letter-exchange"`
}

func (a Arguments) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	if a.MessageTTL > 0 {
		result["x-message-ttl"] = a.MessageTTL
	}
	if a.DeadLetterExchange != "" {
		result["x-dead-letter-exchange"] = a.DeadLetterExchange
	}
	return result
}

type Configuration struct {
	URL       string     `yaml:"url"`
	Node      string     `yaml:"node"`
	Exchanges []Exchange `yaml:"exchanges"`
	Queues    []Queue    `yaml:"queues"`
}

func (config *Configuration) GetNodeName() string {
	if config.Node == "" {
		return "node"
	}
	return config.Node
}

package kafka

import "github.com/philipparndt/go-logger"

type EventConsumer struct {
	Type    string
	Consume func(payload []byte) error
}
type EventRegistry struct {
	Events map[string]EventConsumer
}

var Events = &EventRegistry{
	Events: make(map[string]EventConsumer),
}

func (registry *EventRegistry) Register(consumer EventConsumer) {
	registry.Events[consumer.Type] = consumer
}

func (registry *EventRegistry) Consume(typeName string, event []byte) {
	if consumer, ok := registry.Events[typeName]; ok {
		err := consumer.Consume(event)
		if err != nil {
			logger.Error("Error consuming event:", err)
			return
		}
	} else {
		logger.Error("No consumer for event type:", typeName)
	}

}

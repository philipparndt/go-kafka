package single

import (
	"github.com/IBM/sarama"
	"github.com/philipparndt/go-kafka/kafka"
	"github.com/philipparndt/go-logger"
	"os"
	"sync"
)

func StartConsumer(cfg kafka.KafkaConf, topic string) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go start(&wg, cfg, topic)
	wg.Wait()
}

const startEarlyOffset = -10

func start(wg *sync.WaitGroup, cfg kafka.KafkaConf, topic string) {
	partitionConsumer, schemaRegistryClient, err := cfg.StartConsume(topic)
	if err != nil {
		logger.Error("Error creating consumer:", err)
		os.Exit(1)
		return
	}

	initialized := false
	initialOffset := partitionConsumer.HighWaterMarkOffset() + startEarlyOffset

	go func(topic string, pc sarama.PartitionConsumer) {
		for {
			select {
			case msg := <-pc.Messages():
				if !initialized && msg.Offset >= initialOffset {
					initialized = true
					wg.Done()
				}

				payload, s, err := kafka.DeserializePayload(schemaRegistryClient, msg.Value)
				if err != nil {
					logger.Error("Failed to deserialize payload", err, s)
					return
				}

				name, err := kafka.SchemaName(s)
				if err != nil {
					return
				}
				logger.Debug("Offset", msg.Offset)
				kafka.Events.Consume(name, payload)
			case err := <-pc.Errors():
				logger.Error("Error:", err)
			}
		}
	}(topic, partitionConsumer)
}

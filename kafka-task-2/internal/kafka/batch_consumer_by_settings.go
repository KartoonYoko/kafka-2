package kafka

import (
	"context"
	"fmt"
	"log"

	pb "kafka-task-2/internal/proto"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

// Косьюмер, который автоматически сохраняет смещение, но сконфигурирован для получения ~10 сообщений за раз
type BatchConsumerBySettings struct {
	consumer     *confluent.Consumer
	deserializer *protobuf.Deserializer
}

func NewBatchConsumerBySettings(schemaRegistryURL, bootstrapServers, topic, groupID string) (*BatchConsumerBySettings, error) {
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании клиента Schema Registry: %w", err)
	}

	consumer, err := confluent.NewConsumer(&confluent.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"auto.offset.reset": "latest",
		"fetch.min.bytes":   1600,
		"fetch.wait.max.ms": 60000,
	})
	if err != nil {
		return nil, err
	}

	deserializer, err := protobuf.NewDeserializer(srClient, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		consumer.Close()
		return nil, err
	}

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		deserializer.Close()
		consumer.Close()
		return nil, err
	}

	return &BatchConsumerBySettings{
		consumer:     consumer,
		deserializer: deserializer,
	}, nil
}

func (c *BatchConsumerBySettings) Close() {
	c.deserializer.Close()
	c.consumer.Close()
}

func (c *BatchConsumerBySettings) Receive(ctx context.Context) (*pb.Message, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		ev := c.consumer.Poll(1000)
		if ev == nil {
			log.Println("получено пустое событие от poll")
			continue
		}

		switch e := ev.(type) {
		case *confluent.Message:
			msg := &pb.Message{}
			if err := c.deserializer.DeserializeInto(*e.TopicPartition.Topic, e.Value, msg); err != nil {
				return nil, err
			}

			log.Printf("сообщение получено: топик=%s партиция=%d офсет=%v",
				*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)

			return msg, nil
		case confluent.Error:
			if e.IsFatal() {
				return nil, fmt.Errorf("ошибка чтения сообщения: %w", e)
			}
			log.Printf("ошибка консьюмера: %v", e)
		}
	}
}

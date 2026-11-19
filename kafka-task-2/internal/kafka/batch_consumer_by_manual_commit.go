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

// Косьюмер, у котого отключено автоматическое сохранение смещения
type BatchConsumerByManualCommit struct {
	consumer     *confluent.Consumer
	deserializer *protobuf.Deserializer
}

func NewBatchConsumerByManualCommit(schemaRegistryURL, bootstrapServers, topic, groupID string) (*BatchConsumerByManualCommit, error) {
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании клиента Schema Registry: %w", err)
	}

	consumer, err := confluent.NewConsumer(&confluent.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           groupID,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": false, // смещения сохраняет приложение, а не клиент

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

	return &BatchConsumerByManualCommit{
		consumer:     consumer,
		deserializer: deserializer,
	}, nil
}

func (c *BatchConsumerByManualCommit) Close() {
	c.deserializer.Close()
	c.consumer.Close()
}

func (c *BatchConsumerByManualCommit) Receive(ctx context.Context) (*pb.Message, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		ev := c.consumer.Poll(1000)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *confluent.Message:
			msg := &pb.Message{}
			if err := c.deserializer.DeserializeInto(*e.TopicPartition.Topic, e.Value, msg); err != nil {
				return nil, NewDeserializationError(err)
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

func (c *BatchConsumerByManualCommit) Commit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	partitions, err := c.consumer.Commit()
	if err != nil {
		if kafkaErr, ok := err.(confluent.Error); ok && kafkaErr.Code() == confluent.ErrNoOffset {
			return nil
		}
		return fmt.Errorf("ошибка коммита смещения: %w", err)
	}

	log.Printf("смещение сохранено: %v", partitions)
	return nil
}

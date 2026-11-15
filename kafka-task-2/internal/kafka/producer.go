package kafka

import (
	"fmt"
	"log"
	"time"

	pb "kafka-task-2/internal/proto"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

type Producer struct {
	producer   *confluent.Producer
	serializer *protobuf.Serializer
	topic      string
	partition  int32
}

func NewProducer(schemaRegistryURL, bootstrapServers, topic string, partition int) (*Producer, error) {
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании клиента Schema Registry: %w", err)
	}

	producer, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		return nil, err
	}

	serializer, err := protobuf.NewSerializer(srClient, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		producer.Close()
		return nil, err
	}

	return &Producer{
		producer:   producer,
		serializer: serializer,
		topic:      topic,
		partition:  int32(partition),
	}, nil
}

func (c *Producer) Close() {
	c.serializer.Close()
	c.producer.Close()
}

func (c *Producer) Send(msg *pb.Message) error {
	payload, err := c.serializer.Serialize(c.topic, msg)
	if err != nil {
		return err
	}

	topic := c.topic
	kafkaMsg := &confluent.Message{
		TopicPartition: confluent.TopicPartition{
			Topic:     &topic,
			Partition: c.partition,
		},
		Key:   []byte(msg.GetKey()),
		Value: payload,
	}
	// Канал доставки событий (информации об отправленном сообщении)
	deliveryChan := make(chan confluent.Event)
	defer close(deliveryChan)

	err = c.producer.Produce(kafkaMsg, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan

	// Приводим Events к типу *kafka.Message
	m := e.(*confluent.Message)
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("ошибка доставки сообщения: %w", m.TopicPartition.Error)
	}

	log.Printf("Сообщение отправлено: ключ=%s значение=%s создано=%s топик=%s партиция=%d офсет=%v",
		msg.GetKey(), msg.GetValue(), msg.GetCreatedAt().AsTime().UTC().Format(time.RFC3339),
		*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	return nil
}

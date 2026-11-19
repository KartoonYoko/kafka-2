package app

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
)

type Config struct {
	// SchemaRegistryServer — адрес Schema Registry: по нему клиент берёт схему protobuf, чтобы сериализовать и читать сообщения.
	SchemaRegistryServer string `json:"schema_registry"`
	// KafkaBootstrapServer — адреса брокеров, с которых клиент узнаёт состав кластера и подключается к Kafka.
	KafkaBootstrapServer string `json:"bootstrap"`
	// TopicName — топик, в который продюсер пишет сообщения и из которого их читают консьюмеры.
	TopicName string `json:"topic"`
	// Producer — режим отправки сообщений; включается отдельно от консьюмеров.
	Producer ProducerConfig `json:"producer"`
	// Consumer — обычное чтение: по одному сообщению, offset коммитится клиентом автоматически.
	Consumer ConsumerConfig `json:"consumer"`
	// BatchConsumerBySettings — пакетное чтение за счёт fetch.min.bytes и fetch.wait.max.ms; offset по-прежнему коммитится автоматически.
	BatchConsumerBySettings BatchConsumerConfig `json:"batch_consumer_by_settings"`
	// BatchConsumerByManualCommit — пакетное чтение, при котором offset фиксирует само приложение после накопления пачки.
	BatchConsumerByManualCommit ManualBatchConsumerConfig `json:"batch_consumer_by_manual_commit"`
}

type ProducerConfig struct {
	// Enabled — запускать ли продюсер в этом процессе. Режимы независимы, поэтому в одном запуске можно включить только нужные.
	Enabled bool `json:"enabled"`
	// Delay — пауза в секундах между отправками, чтобы поток сообщений было видно в логах и Kafka UI.
	Delay int `json:"delay"`
	// MaxMessages — сколько сообщений отправить и остановиться. 0 означает слать без ограничения, пока процесс не остановят.
	MaxMessages int `json:"max_messages"`
}

type ConsumerConfig struct {
	// Enabled — запускать ли обычного консьюмера в этом процессе.
	Enabled bool `json:"enabled"`
	// GroupID — группа потребителей. Kafka хранит offset отдельно для каждой группы, поэтому режимы не сдвигают позицию друг друга.
	GroupID string `json:"group"`
}

type BatchConsumerConfig struct {
	// Enabled — запускать ли консьюмера, который ждёт пачку за счёт настроек fetch.
	Enabled bool `json:"enabled"`
	// GroupID — отдельная группа, чтобы offset этого режима не смешивался с остальными консьюмерами.
	GroupID string `json:"group"`
}

type ManualBatchConsumerConfig struct {
	// Enabled — запускать ли консьюмера с ручным коммитом offset.
	Enabled bool `json:"enabled"`
	// GroupID — отдельная группа: позиция чтения этого режима хранится независимо от остальных.
	GroupID string `json:"group"`
	// BatchSize — сколько сообщений обработать, прежде чем зафиксировать offset. Коммит идёт пачкой, а не после каждого сообщения.
	BatchSize int `json:"batch_size"`
}

func NewConfig() (*Config, error) {
	c := new(Config)

	if err := c.load(); err != nil {
		return nil, err
	}

	return c, nil
}

// load читает JSON-конфиг, затем перекрывает его флагами, которые реально переданы в командной строке.
func (c *Config) load() error {
	configPath := flag.String("config", "config.json", "Путь к файлу конфигурации")
	schemaRegistryServer := flag.String("schema-registry", "", "URL Schema Registry")
	kafkaBootstrapServer := flag.String("bootstrap", "", "Адреса bootstrap-серверов Kafka")
	topic := flag.String("topic", "", "Имя топика Kafka")
	delay := flag.Int("delay", 0, "Задержка в секундах перед следующим сообщением")
	maxMessages := flag.Int("max-messages", 0, "Максимальное число сообщений для отправки; 0 — без ограничения")
	groupID := flag.String("group", "", "Идентификатор группы потребителей Kafka")
	producerEnabled := flag.Bool("producer", false, "Включить producer")
	consumerEnabled := flag.Bool("consumer", false, "Включить consumer")
	batchSettingsEnabled := flag.Bool("batch-by-settings", false, "Включить пакетного потребителя с настройками fetch")
	batchSettingsGroup := flag.String("batch-settings-group", "", "Группа потребителей для пакетного чтения по настройкам fetch")
	batchManualEnabled := flag.Bool("batch-by-manual-commit", false, "Включить пакетного потребителя с ручным коммитом offset")
	batchManualGroup := flag.String("batch-manual-group", "", "Группа потребителей для пакетного чтения с ручным коммитом")
	batchSize := flag.Int("batch-size", 0, "Число сообщений перед ручным коммитом offset")

	flag.Parse()

	if err := c.loadFile(*configPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) || configFlagSet() {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("чтение конфига %s: %w", *configPath, err)
			}
			return err
		}
	}

	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "schema-registry":
			c.SchemaRegistryServer = *schemaRegistryServer
		case "bootstrap":
			c.KafkaBootstrapServer = *kafkaBootstrapServer
		case "topic":
			c.TopicName = *topic
		case "delay":
			c.Producer.Delay = *delay
		case "max-messages":
			c.Producer.MaxMessages = *maxMessages
		case "group":
			c.Consumer.GroupID = *groupID
		case "producer":
			c.Producer.Enabled = *producerEnabled
		case "consumer":
			c.Consumer.Enabled = *consumerEnabled
		case "batch-by-settings":
			c.BatchConsumerBySettings.Enabled = *batchSettingsEnabled
		case "batch-settings-group":
			c.BatchConsumerBySettings.GroupID = *batchSettingsGroup
		case "batch-by-manual-commit":
			c.BatchConsumerByManualCommit.Enabled = *batchManualEnabled
		case "batch-manual-group":
			c.BatchConsumerByManualCommit.GroupID = *batchManualGroup
		case "batch-size":
			c.BatchConsumerByManualCommit.BatchSize = *batchSize
		}
	})

	if !c.Producer.Enabled && !c.Consumer.Enabled && !c.BatchConsumerBySettings.Enabled && !c.BatchConsumerByManualCommit.Enabled {
		return fmt.Errorf("producer и consumer выключены")
	}
	if c.BatchConsumerByManualCommit.Enabled && c.BatchConsumerByManualCommit.BatchSize < 1 {
		return fmt.Errorf("batch_size должен быть больше 0")
	}

	return nil
}

func configFlagSet() bool {
	set := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			set = true
		}
	})
	return set
}

func (c *Config) loadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return err
		}
		return fmt.Errorf("чтение конфига %s: %w", path, err)
	}

	if err := json.Unmarshal(data, c); err != nil {
		return fmt.Errorf("разбор конфига %s: %w", path, err)
	}

	return nil
}

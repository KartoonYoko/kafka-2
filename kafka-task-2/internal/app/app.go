package app

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"kafka-task-2/internal/kafka"
)

func Run() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conf, err := NewConfig()
	if err != nil {
		log.Fatalf("ошибка конфигурации: %s", err)
	}

	var producer *kafka.Producer
	var consumer *kafka.Consumer
	var batchBySettings *kafka.BatchConsumerBySettings
	var batchByManualCommit *kafka.BatchConsumerByManualCommit

	closeClients := func() {
		if producer != nil {
			producer.Close()
		}
		if consumer != nil {
			consumer.Close()
		}
		if batchBySettings != nil {
			batchBySettings.Close()
		}
		if batchByManualCommit != nil {
			batchByManualCommit.Close()
		}
	}

	if conf.Producer.Enabled {
		producer, err = kafka.NewProducer(conf.SchemaRegistryServer, conf.KafkaBootstrapServer, conf.TopicName)
		if err != nil {
			log.Fatalf("ошибка продюсера: %s", err)
		}
	}

	if conf.Consumer.Enabled {
		consumer, err = kafka.NewConsumer(conf.SchemaRegistryServer, conf.KafkaBootstrapServer, conf.TopicName, conf.Consumer.GroupID)
		if err != nil {
			closeClients()
			log.Fatalf("ошибка консьюмера: %s", err)
		}
	}

	if conf.BatchConsumerBySettings.Enabled {
		batchBySettings, err = kafka.NewBatchConsumerBySettings(conf.SchemaRegistryServer, conf.KafkaBootstrapServer, conf.TopicName, conf.BatchConsumerBySettings.GroupID)
		if err != nil {
			closeClients()
			log.Fatalf("ошибка пакетного консьюмера по настройкам: %s", err)
		}
	}

	if conf.BatchConsumerByManualCommit.Enabled {
		batchByManualCommit, err = kafka.NewBatchConsumerByManualCommit(conf.SchemaRegistryServer, conf.KafkaBootstrapServer, conf.TopicName, conf.BatchConsumerByManualCommit.GroupID)
		if err != nil {
			closeClients()
			log.Fatalf("ошибка пакетного консьюмера с ручным коммитом: %s", err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	if producer != nil {
		sendMessage := NewSendMessage(producer, conf.Producer.Delay, conf.Producer.MaxMessages)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sendMessage.Execute(ctx); err != nil {
				stop()
				errCh <- err
			}
		}()
	}
	if consumer != nil {
		receiveMessage := NewReceiveMessage(consumer)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := receiveMessage.Execute(ctx); err != nil {
				stop()
				errCh <- err
			}
		}()
	}
	if batchBySettings != nil {
		receiveMessage := NewReceiveMessage(batchBySettings)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := receiveMessage.Execute(ctx); err != nil {
				stop()
				errCh <- err
			}
		}()
	}
	if batchByManualCommit != nil {
		receiveBatch := NewReceiveBatchMessagesByManualCommit(batchByManualCommit, conf.BatchConsumerByManualCommit.BatchSize)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := receiveBatch.Execute(ctx); err != nil {
				stop()
				errCh <- err
			}
		}()
	}

	wg.Wait()
	closeClients()
	close(errCh)

	for err := range errCh {
		log.Fatalf("ошибка выполнения: %s", err)
	}
}

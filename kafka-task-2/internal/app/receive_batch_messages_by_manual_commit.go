package app

import (
	"context"
	"errors"
	"log"
	"time"

	"kafka-task-2/internal/kafka"
	pb "kafka-task-2/internal/proto"
)

type ReceiverByManualCommit interface {
	Receive(ctx context.Context) (*pb.Message, error)
	Commit(ctx context.Context) error
}

type ReceiveBatchMessagesByManualCommit struct {
	receiver  ReceiverByManualCommit
	batchSize int
}

func NewReceiveBatchMessagesByManualCommit(receiver ReceiverByManualCommit, batchSize int) *ReceiveBatchMessagesByManualCommit {
	return &ReceiveBatchMessagesByManualCommit{
		receiver:  receiver,
		batchSize: batchSize,
	}
}

func (r *ReceiveBatchMessagesByManualCommit) Execute(ctx context.Context) error {
	counter := 0
	for {
		if stopped(ctx) {
			return nil
		}

		msg, err := r.receiver.Receive(ctx)
		if err != nil {
			var deserializationErr *kafka.DeserializationError
			if errors.As(err, &deserializationErr) {
				log.Printf("не удалось десериализовать сообщение: %v", deserializationErr)
				continue
			}
			if stopped(ctx) {
				return nil
			}
			return err
		}

		log.Printf("получено сообщение: uuid %s, date %s, value %s",
			msg.GetKey(),
			msg.GetCreatedAt().AsTime().UTC().Format(time.RFC3339),
			msg.GetValue(),
		)

		counter++

		if counter%r.batchSize == 0 {
			if err := r.receiver.Commit(ctx); err != nil {
				if stopped(ctx) {
					return nil
				}
				return err
			}
		}
	}
}

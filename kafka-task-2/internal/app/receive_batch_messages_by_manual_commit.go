package app

import (
	"context"
	"log"
	"time"

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

		if _, err := r.receiver.Receive(ctx); err != nil {
			if stopped(ctx) {
				return nil
			}
			return err
		}

		counter++

		if counter%r.batchSize == 0 {
			msg, err := r.receiver.Receive(ctx)
			if err != nil {
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
		}
	}
}

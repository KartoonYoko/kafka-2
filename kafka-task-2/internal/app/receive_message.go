package app

import (
	"context"
	"log"
	"time"

	pb "kafka-task-2/internal/proto"
)

type Receiver interface {
	Receive(ctx context.Context) (*pb.Message, error)
}

type ReceiveMessage struct {
	receiver Receiver
}

func NewReceiveMessage(receiver Receiver) *ReceiveMessage {
	return &ReceiveMessage{
		receiver: receiver,
	}
}

func (r *ReceiveMessage) Execute(ctx context.Context) error {
	for {
		if stopped(ctx) {
			return nil
		}

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

package app

import (
	"context"
	"fmt"
	"time"

	pb "kafka-task-2/internal/proto"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Sender interface {
	Send(msg *pb.Message) error
}

type SendMessage struct {
	sender      Sender
	delay       time.Duration
	maxMessages int
}

func NewSendMessage(sender Sender, delaySeconds, maxMessages int) *SendMessage {
	return &SendMessage{
		sender:      sender,
		delay:       time.Duration(delaySeconds) * time.Second,
		maxMessages: maxMessages,
	}
}

func (s *SendMessage) Execute(ctx context.Context) error {
	for n := 1; ; n++ {
		if stopped(ctx) {
			return nil
		}

		msg := &pb.Message{
			Key:       uuid.NewString(),
			CreatedAt: timestamppb.New(time.Now().UTC()),
			Value:     fmt.Sprintf("hello-%d", n),
		}
		if err := s.sender.Send(msg); err != nil {
			if stopped(ctx) {
				return nil
			}
			return err
		}
		if s.maxMessages > 0 && n >= s.maxMessages {
			return nil
		}
		if err := s.wait(ctx); err != nil {
			stopped(ctx)
			return nil
		}
	}
}

func (s *SendMessage) wait(ctx context.Context) error {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

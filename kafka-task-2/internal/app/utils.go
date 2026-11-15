package app

import (
	"context"
	"log"
)

func stopped(ctx context.Context) bool {
	if ctx.Err() == nil {
		return false
	}
	log.Println("получен сигнал завершения, останавливаем работу")
	return true
}

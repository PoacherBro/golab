package kafka_test

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/PoacherBro/golab/kafka"
	"github.com/Shopify/sarama"
)

type tmpMsgHandler struct {
	log *testing.T
}

func (t *tmpMsgHandler) Consume(msg *kafka.ConsumerMessage) error {
	logMsg := fmt.Sprintf("Receive topic[%s]-partition[%d]-offset[%d] message (key=%s, value=[%s]",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

	t.log.Log(logMsg)
	log.Println(logMsg)

	return nil
}

func initConsumer(t *testing.T) (*kafka.Consumer, error) {
	cfg := &kafka.ConsumerConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    []string{"test1"},
		MaxRetry: 5,
		GroupID:  "kafka-test",
		Workers:  3,
	}
	handler := &tmpMsgHandler{log: t}
	return kafka.NewConsumer(cfg, handler)
}

func TestConsumer(t *testing.T) {
	log.SetFlags(log.Ldate | log.Lshortfile)
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	consumer, err := initConsumer(t)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Kafka Consumer close failed, %v", err)
		}
	}()

	// Create signal channel
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go consumer.Consume()

	<-sigchan
	log.Println("Receive signal, exited")
}

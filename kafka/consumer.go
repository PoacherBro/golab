package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

const (
	defaultPartitionConsumerWorkers = 1
)

// ConsumerMessage struct of passed to message handler
type ConsumerMessage struct {
	*sarama.ConsumerMessage
}

// MessageHandler registed by application to process message, each consumer has one message handler.
// Application should handle different message struct per different topic if he consumes multiple topics.
// Returned error means consume failed, then message will be retied ConsumerConfig.MaxRetry times.
type MessageHandler interface {
	Consume(msg *ConsumerMessage) error
}

// Consumer kafka consumer group
// Group means a biz unit, should process the related biz based on subscribed topics
type Consumer struct {
	clientName string
	cfg        *ConsumerConfig
	consumer   sarama.ConsumerGroup
	msgHandler MessageHandler
	pc         *partitionConsumer
	ctx        context.Context
	cancelCtx  context.CancelFunc
}

// NewConsumer create a consumer to subscribe many topics
func NewConsumer(cfg *ConsumerConfig, handler MessageHandler) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	clientName := generateClientID(cfg.GroupID)
	config.ClientID = clientName
	config.Version = sarama.V1_1_0_0
	c, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, config)
	if err != nil {
		log.Printf("Kafka Consumer: [%s] init fail, %v", clientName, err)
		return nil, err
	}

	validConfigValue(cfg)
	consumer := &Consumer{
		clientName: clientName,
		cfg:        cfg,
		consumer:   c,
		msgHandler: handler,
	}
	consumer.ctx, consumer.cancelCtx = context.WithCancel(context.Background())
	consumer.pc = consumer.newPartitionConsumer(handler)
	log.Printf("Kafka Consumer: [%s] init success", clientName)
	go consumer.reportError()

	return consumer, nil
}

// generate client ID for debug, default use groupID-hostName,
// but if get host name failed, then use the unix nano time
func generateClientID(groupID string) string {
	hostName, err := os.Hostname()
	if err != nil || len(hostName) == 0 {
		now := time.Now().UnixNano()
		hostName = strconv.FormatInt(now, 10)
	}
	return fmt.Sprintf("%s-%s", groupID, hostName)
}

func validConfigValue(cfg *ConsumerConfig) {
	if cfg.Workers <= 0 {
		cfg.Workers = defaultPartitionConsumerWorkers
	}
	if cfg.MaxRetry < 0 {
		cfg.MaxRetry = 0
	}
}

func (c *Consumer) reportError() {
	for err := range c.consumer.Errors() {
		if cerr, ok := err.(*sarama.ConsumerError); ok {
			log.Println("Kafka Consumer: consume failed", cerr.Error())
		} else {
			log.Println("Kafka Consumer: receive unknown err", err)
		}
	}
}

// Close consumer
func (c *Consumer) Close() error {
	c.cancelCtx()
	err := c.consumer.Close()
	log.Printf("Kafka Consumer: [%s] consumer quit", c.clientName)
	return err
}

// Consume message per partition, each partition have numbers of workers
func (c *Consumer) Consume() {
	log.Printf("Kafka Consumer: [%s] start consume", c.clientName)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := c.consumer.Consume(c.ctx, c.cfg.Topic, c.pc); err != nil {
				log.Println("Kafka Consumer: consume err", err)
			}
		}
	}
}

type partitionConsumer struct {
	handler MessageHandler
	parent  *Consumer
}

func (c *Consumer) newPartitionConsumer(handler MessageHandler) *partitionConsumer {
	return &partitionConsumer{
		handler: handler,
		parent:  c,
	}
}

func (pc *partitionConsumer) Setup(sess sarama.ConsumerGroupSession) error {
	return nil
}

func (pc *partitionConsumer) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}

func (pc *partitionConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		for i := 0; i < pc.parent.cfg.MaxRetry+1; i++ {
			// quick first
			select {
			case <-sess.Context().Done():
				return nil
			default:
			}

			err := pc.handler.Consume(&ConsumerMessage{msg})
			if err == nil {
				break
			}

			select {
			case <-sess.Context().Done():
				return nil
			default:
			}
			// release CPU and avoid network issue
			increaseDuration := time.Duration((i + 1) * 300)
			time.Sleep(increaseDuration * time.Millisecond)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

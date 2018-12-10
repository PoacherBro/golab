package kafka

import (
	"context"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

type consumerGroupMsgHandler struct {
	msgHanlder    MessageHanlder
	consumerGroup *ConsumerGroup
}

func (consumerGroupMsgHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupMsgHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupMsgHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		for i := 0; i < h.consumerGroup.cfg.MaxRetry; i++ {
			err := h.msgHanlder.Consume(&ConsumerMessage{msg})
			if err == nil {
				break
			}
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

// ConsumerGroup instance of high level consumer group
type ConsumerGroup struct {
	clientName string
	cfg        *ConsumerConfig
	cg         *sarama.ConsumerGroup
	handler    MessageHanlder
}

// NewConsumerGroup init a sarama consumer-group
func NewConsumerGroup(cfg *ConsumerConfig, handler MessageHanlder) (*ConsumerGroup, error) {
	c := sarama.NewConfig()
	c.Version = sarama.V0_10_2_0
	c.Metadata.RefreshFrequency = 1 * time.Minute
	c.Consumer.Offsets.Initial = sarama.OffsetNewest
	c.Consumer.Return.Errors = true
	clientName := generateClientID(cfg.GroupID)
	c.ClientID = clientName
	cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, c)
	if err != nil {
		return nil, err
	}
	go func() {
		for err := range cg.Errors() {
			log.Printf("Consumer Group: [%s] consumer err %v", cfg.GroupID, err)
		}
	}()
	h := consumerGroupMsgHandler{msgHanlder: handler}
	consumerGroup := &ConsumerGroup{
		clientName: clientName,
		cfg:        cfg,
		cg:         &cg,
		handler:    handler,
	}
	go func() {
		err := cg.Consume(context.Background(), cfg.Topic, h)
		if err != nil {
			log.Println(err)
		}
	}()
	return consumerGroup, nil
}

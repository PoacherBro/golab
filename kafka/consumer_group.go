package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/Shopify/sarama"
)

// Client Kafka client
type Client struct {
	client sarama.Client
}

// ConsumerGroup Kafka consumer groupq
type ConsumerGroup struct {
	groupID      string
	topics       []string
	client       *Client
	cosumerGroup sarama.ConsumerGroup
	groupHanlder *consumerGroupHandler
}

// NewClient create a new Kafka client
func NewClient(cfg *ConsumerConfig) (*Client, error) {
	config := sarama.NewConfig()
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	config.Version = sarama.V1_1_0_0
	clientID := generateClientID(cfg.GroupID)
	config.ClientID = clientID
	client, err := sarama.NewClient(cfg.Brokers, config)
	if err != nil {
		return nil, err
	}
	c := &Client{
		client: client,
	}
	return c, nil
}

// NewConsumerGroupFromClient create consumer group from a give client
// PLEASE NOTE: consumer group can reuse the client but NOT share the client based on Kafka specification
func NewConsumerGroupFromClient(groupID string, topics []string, client *Client) (*ConsumerGroup, error) {
	cg, err := sarama.NewConsumerGroupFromClient(groupID, client.client)
	if err != nil {
		return nil, err
	}
	consumerGroup := &ConsumerGroup{
		groupID:      groupID,
		topics:       topics,
		client:       client,
		cosumerGroup: cg,
		groupHanlder: &consumerGroupHandler{
			msgHandlers: make(map[string]*msgHandler),
		},
	}
	consumerGroup.groupHanlder.parent = consumerGroup
	return consumerGroup, nil
}

// RegisterHandler rgister handler for each topic-key
// Please NOTE: if there is a existed registered handler, will return error
func (c *ConsumerGroup) RegisterHandler(topic, key string, encoder Encoder, handler ConsumerMessageHandler) error {
	var err error

	msgHandler := &msgHandler{
		handlers: make(map[string]ConsumerMessageHandler),
		topic:    topic,
		encoder:  encoder,
	}
	if keyHandler, ok := c.groupHanlder.msgHandlers[topic]; ok {
		if _, exists := keyHandler.handlers[key]; exists {
			err = fmt.Errorf("Consumer Group [%s] register same handler topic[%s]-key[%s]", c.groupID, topic, key)
			return err
		}
		msgHandler = keyHandler
	} else {
		c.groupHanlder.msgHandlers[topic] = msgHandler
	}

	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("Consumer Group [%s] register with err=%v", c.groupID, e)
			log.Fatalln(err)
		}
	}()

	t := reflect.TypeOf(handler)
	paramCount := t.NumIn()
	msgType := t.In(paramCount - 1)
	value := reflect.New(msgType).Interface()
	msgHandler.handlers[key] = handler
	msgHandler.encoder = encoder
	msgHandler.entity = value
	return err
}

// Consume will start partition consumer for listen topics
func (c *ConsumerGroup) Consume() {
	for {
		c.cosumerGroup.Consume(context.Background(), c.topics, c.groupHanlder)
	}
}

// Close consumer group, if its related client not closed, also close
func (c *ConsumerGroup) Close() error {
	if !c.client.client.Closed() {
		c.client.client.Close()
	}
	return c.cosumerGroup.Close()
}

type consumerGroupHandler struct {
	parent      *ConsumerGroup
	msgHandlers map[string]*msgHandler
}

// ConsumerMessageHandler registered handler for each message process
type ConsumerMessageHandler func(ctx context.Context, topic, key string, msg interface{}) error

type msgHandler struct {
	topic      string
	entity     interface{}
	encoder    Encoder
	handlers   map[string]ConsumerMessageHandler // msgType -> handler
	maxRetries int
}

func (h *consumerGroupHandler) Setup(ccs sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) Cleanup(ccs sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		topic := claim.Topic()
		msgHandler, ok := h.msgHandlers[topic]
		if !ok {
			err := fmt.Errorf("Consumer Group [%s]: no handler for topic[%s]", h.parent.groupID, topic)
			log.Fatalln(err)
			return err
		}
		consumerMsg := &Message{}
		err := json.Unmarshal(msg.Value, consumerMsg)
		if err != nil {
			log.Fatalf("Consumer Group: unexpected message, err=%v\n", err)
			return err
		}
		entity := msgHandler.entity
		err = msgHandler.encoder.Decode(consumerMsg.Entity, entity)
		if err != nil {
			log.Fatalf("Consumer Group: handler decode value failed, err=%v\n", err)
			continue
		}
		fn, ok := msgHandler.handlers[consumerMsg.Type]
		if !ok {
			log.Printf("Consumer Group: no handler for topic[%s]-msgType[%s]", topic, consumerMsg.Type)
			continue
		}
		fn(sess.Context(), topic, consumerMsg.Type, entity)
		sess.MarkMessage(msg, "")
	}
	return nil
}

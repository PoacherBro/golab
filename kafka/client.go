package kafka

import (
	"time"

	cluster "github.com/bsm/sarama-cluster"
)

// Client create kafka client instance, each application should be just one
// if create multiple client, will return in-user error
type Client struct {
	cfg           *ClientConfig
	clusterClient *cluster.Client
}

// ClientConfig setup kafka client, should not be changed once a client created
type ClientConfig struct {
	Brokers []string
}

// NewClient return a kafka client, sigletone instance per one application.
// Client can be reused in producer/consumer.
func NewClient(cfg *ClientConfig) (*Client, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Net.DialTimeout = 3 * time.Second
	config.Net.ReadTimeout = 3 * time.Second
	config.Net.WriteTimeout = 3 * time.Second

	client, err := cluster.NewClient(cfg.Brokers, config)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:           cfg,
		clusterClient: client,
	}, nil
}

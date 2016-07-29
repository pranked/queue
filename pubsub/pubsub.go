package pubsub

import (
	"encoding/json"

	"github.com/pranked/queue"
	"github.com/pranked/tree"
	"github.com/streadway/amqp"
)

const (
	PubSubCorr = "PL-PUBSUB"
)

type Client struct {
	channel *queue.Channel
	topics  *tree.TreeNode
}

type EventHandler func(data *queue.Data) error

func New(c *queue.Channel) *Client {
	pb := &Client{
		channel: c,
		topics:  tree.New(),
	}
	c.UseTap(pb)
	return pb
}

// On will run the given Handler when a delivery on the topic provided is found.
// It should be noted that topic.* cannot be used here, and that one topic can only have one handler.
func (c *Client) On(topic string, fn EventHandler) {
	c.topics.Create(topic).Object = fn
}

// Publish will send the specified publishing object to the topic provided like logs.critical
func (c *Client) PublishRaw(topic string, data []byte) error {
	return c.channel.Publish(topic, amqp.Publishing{
		Body:          data,
		CorrelationId: PubSubCorr,
		ContentType:   queue.BinaryContentType,
	})
}

// Publish will send the specified publishing object to the topic provided like logs.critical
func (c *Client) PublishObj(topic string, obj interface{}) error {
	buf, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return c.channel.Publish(topic, amqp.Publishing{
		Body:          buf,
		CorrelationId: PubSubCorr,
		ContentType:   queue.JsonContentType,
	})
}

func (ps Client) Tap(d *queue.Data) error {
	if d.Delivery.CorrelationId != PubSubCorr {
		return nil
	}
	node := ps.topics.Search(d.Delivery.RoutingKey)
	if node != nil {
		// Run function if err not nil don't accept
		err := (node.Object.(EventHandler))(d)
		if err != nil {
			// Todo handle error
			d.Nack()
		}
	}
	return nil
}

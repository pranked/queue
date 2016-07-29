package queue

import (
	"github.com/streadway/amqp"
	"fmt"
)

type Channel struct {
	Chan     *amqp.Channel
	exchange string
	Queue    amqp.Queue
	taps     []Tap
	Closed   bool
}

type Tap interface {
	Tap(d *Data) error
}

func (c *Channel) init() error {
	// Declare the exchange name
	if err := c.Chan.ExchangeDeclare(c.exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	q, err := c.Chan.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		return err
	}
	c.Queue = q
	return nil
}

// Publish will send the specified publishing object with the topic provided.
func (c *Channel) Publish(topic string, pub amqp.Publishing) error {
	return c.Chan.Publish(c.exchange, topic, false, false, pub)
}

// Bind will bind the current queue's exchange to topics provided. Eg: logs.critical, logs.kernel, logs.*
func (c *Channel) Bind(topics ...string) error {
	for _, topic := range topics {
		err := c.Chan.QueueBind(
			c.Queue.Name,
			topic,
			c.exchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// Listen is a blocking call that will begin consuming the queue and processing requests.
func (c *Channel) Listen() (error) {
	msgs, err := c.Chan.Consume(c.Queue.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	// Wait for messages
	for msg := range msgs {
		go func () {
			d := &Data{Delivery: msg}
			// For each tap we have give it the data
			for _, tap := range c.taps {
				err = tap.Tap(d)
				if err != nil {
					//todo do something with the error
					return
				}
			}
			// Should we accept the message
			if d.nack {
				fmt.Println("nack", msg.Nack(false, true))
			}
		}()
	}
	return nil
}

func (c *Channel) UseTap(t Tap) {
	c.taps = append(c.taps, t)
}

// Close sends the close notification to the amqp channel, and cleans up resources.
func (c *Channel) Close() error {
	c.Closed = true
	return c.Chan.Close()
}
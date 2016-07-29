package queue

import (
	"github.com/streadway/amqp"
	"encoding/json"
)

const (
	JsonContentType = "application/json"
	BinaryContentType = "application/octect-stream"
	PlainContentType = "text/plain"
)

type Data struct {
	Delivery amqp.Delivery
	nack     bool
}

// Nack will not accept this delivery, error handling is provided up the stack.
func (d *Data) Nack() {
	d.nack = true
}

// ToObject will take the bytes from the Delivery and Marshal them into an interface.
func (d *Data) ToObject(v interface{}) error {
	return json.Unmarshal(d.Delivery.Body, v)
}

// Raw is an alias for Delivery.Body
func (d *Data) Raw() []byte {
	return d.Delivery.Body
}

func (d *Data) String() string {
	return string(d.Delivery.Body)
}
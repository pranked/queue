package queue

import (
	"github.com/streadway/amqp"
	"os"
	"fmt"
)

type Queue struct {
	conn *amqp.Connection
}

// New creates a new RabbitMQ queue client and connection.
func New() (*Queue, error) {
	user := os.Getenv("AMQP_USER")
	pass := os.Getenv("AMQP_PASS")
	end := os.Getenv("AMQP_ENDPOINT")
	connection, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", user, pass, end))
	if err != nil {
		return nil, err
	}
	return &Queue{conn: connection}, nil
}

// Create a new channel to communicate on.
func (q *Queue) Open(exchange string) (*Channel, error) {
	ch, err := q.conn.Channel()
	if err != nil {
		return nil, err
	}
	chAbs := &Channel{Chan: ch, exchange: exchange}
	if err := chAbs.init(); err != nil {
		return nil, err
	}
	return chAbs, nil
}

func (q *Queue) Close() {
	q.conn.Close()
}

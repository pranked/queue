package rpc

import (
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"github.com/Workiva/go-datastructures/set"
	"github.com/pranked/queue"
	"github.com/pranked/tree"
	"github.com/streadway/amqp"
)

const (
	ProcedureCallContentType   = "application/x-plumn-rpc:call"
	ProcedureReturnContentType = "application/x-plumn-rpc:return"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type ProcedureHandler func(args []interface{}) (interface{}, error)

type Client struct {
	channel    *queue.Channel
	procedures *tree.TreeNode
	callbacks  *set.Set
}

type ProcedureResult struct {
	Value interface{}
	Error error
}

func New(channel *queue.Channel) *Client {
	rp := &Client{
		channel:    channel,
		procedures: tree.New(),
		callbacks:  set.New(),
	}
	channel.UseTap(rp)
	return rp
}

func (c *Client) Register(name string, fn ProcedureHandler) {
	c.procedures.Create(name).Object = fn
}

func (c Client) Tap(d *queue.Data) error {
	switch d.Delivery.ContentType {
	case ProcedureCallContentType:
		args := []interface{}{}
		if err := d.ToObject(&args); err != nil {
			return err
		}
		res := c.invokeProcedure(d.Delivery.RoutingKey, args)
		buf, err := json.Marshal(res)
		if err != nil {
			return err
		}
		// Directly publish to the channel of the caller
		c.channel.Chan.Publish("", d.Delivery.ReplyTo, false, false, amqp.Publishing{
			Body:          buf,
			CorrelationId: d.Delivery.CorrelationId,
			ContentType:   ProcedureReturnContentType,
		})
		break
	case ProcedureReturnContentType:
		res := &ProcedureResult{}
		if err := d.ToObject(res); err != nil {
			return err
		}
		for _, obj := range c.callbacks.Flatten() {
			cb := obj.(callback)
			if cb.key == d.Delivery.CorrelationId {
				cb.c <- res
			}
		}
		break
	}
	return nil
}

func (c *Client) invokeProcedure(name string, args []interface{}) ProcedureResult {
	n := c.procedures.Search(name)
	if n == nil {
		return ProcedureResult{Error: errors.New("Procedure not found.")}
	}
	obj, err := n.Object.(ProcedureHandler)(args)
	return ProcedureResult{
		Value: obj,
		Error: err,
	}
}

func (c *Client) Call(name string, args ...interface{}) (chan *ProcedureResult, error) {
	buf, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	// Send call
	cb := c.callback()
	err = c.channel.Publish(name, amqp.Publishing{
		Body:          buf,
		ContentType:   ProcedureCallContentType,
		ReplyTo:       c.channel.Queue.Name,
		CorrelationId: cb.key,
	})
	// It failed so end here
	if err != nil {
		return nil, err
	}
	return cb.c, nil
}

func (c *Client) callback() callback {
	key := ""
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		key = string(time.Now().UnixNano())
	}
	key = string(buf)
	cb := callback{
		c:   make(chan *ProcedureResult),
		key: key,
	}
	c.callbacks.Add(cb)
	go func() {
		time.Sleep(8 * time.Second)
		c.callbacks.Remove(cb)
		cb.c <- nil
		close(cb.c)
	}()
	return cb
}

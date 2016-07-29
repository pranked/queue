package queue

import (
	"testing"
	"time"
)

func TestQueue_Open(t *testing.T) {
	n, err := New()
	if err != nil {
		t.Error(err)
	}
	c, err := n.Open("plumnode.development")
	if err != nil {
		t.Error(err)
	}
	c.Bind("code.*")

	go c.Listen()
	time.Sleep(2 * time.Second)
}
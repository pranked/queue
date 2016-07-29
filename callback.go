package queue

type callback struct {
	key string
	data chan *Data
}

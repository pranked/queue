package rpc

type callback struct {
	key string
	c   chan *ProcedureResult
}

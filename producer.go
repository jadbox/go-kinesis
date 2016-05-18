package kinesis

type Producer interface {
	Put(data []byte) error
	Start()
	Stop()
}

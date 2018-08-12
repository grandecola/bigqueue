package bigqueue

// IBigQueue provides an interface to big, fast and persistent queue
type IBigQueue interface {
	IsEmpty() bool
	Peek() []byte
	Enqueue(elem []byte) error
	Dequeue() ([]byte, error)
}

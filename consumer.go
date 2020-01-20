package bigqueue

// Consumer is a bigqueue consumer that allows reading data from bigqueue.
// A consumer is represented using just a base offset into the metadata
type Consumer struct {
	mq   *MmapQueue
	base int64 // base offset in the metadata file
}

// IsEmpty returns true when queue is empty for the consumer.
func (c *Consumer) IsEmpty() bool {
	return c.mq.isEmpty(c.base)
}

// Dequeue removes an element from the queue and returns it.
func (c *Consumer) Dequeue() ([]byte, error) {
	return c.mq.dequeue(c.base)
}

// DequeueString removes a string element from the queue and returns it.
func (c *Consumer) DequeueString() (string, error) {
	return c.mq.dequeueString(c.base)
}

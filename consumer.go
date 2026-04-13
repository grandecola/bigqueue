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

// BacklogBytes returns the number of bytes in the queue that have not yet been
// consumed by this consumer. The value includes the 8-byte length prefix stored
// before each message payload. Returns ErrInvalidQueueState if the queue
// metadata is inconsistent (tail behind head).
func (c *Consumer) BacklogBytes() (int64, error) {
	return c.mq.backlogBytes(c.base)
}

// Dequeue removes an element from the queue and returns it.
func (c *Consumer) Dequeue() ([]byte, error) {
	return c.mq.dequeue(c.base)
}

// DequeueWithTag removes an element from the queue and returns the message and its
// tag ([]byte). The message was expected to be enqueued via EnqueueWithTag.
func (c *Consumer) DequeueWithTag() ([]byte, []byte, error) {
	return c.mq.dequeueWithTag(c.base)
}

// DequeueString removes a string element from the queue and returns it.
func (c *Consumer) DequeueString() (string, error) {
	return c.mq.dequeueString(c.base)
}

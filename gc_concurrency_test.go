package bigqueue

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestArenaGC_Concurrency(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "test_bigqueue_gc_concurrency")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// Configuration: Small arena to trigger expansion and GC frequently
	arenaSize := 4096
	maxKeep := 2
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(maxKeep))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	const (
		numProducers    = 3
		numConsumers    = 2
		msgsPerProducer = 200
		msgSize         = 512 // Approx 8 messages per arena
	)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	// 1. Start Producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			<-startSignal
			for j := 0; j < msgsPerProducer; j++ {
				msg := []byte(fmt.Sprintf("p%d-m%d", pid, j))
				payload := make([]byte, msgSize)
				copy(payload, msg)
				if err := bq.Enqueue(payload); err != nil {
					t.Errorf("Producer %d failed: %v", pid, err)
					return
				}
			}
		}(i)
	}

	// 2. Start Consumers
	counts := make([]int, numConsumers)
	var countMu sync.Mutex
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			<-startSignal
			c, err := bq.NewConsumer(fmt.Sprintf("c%d", cid))
			if err != nil {
				t.Errorf("Consumer %d failed to init: %v", cid, err)
				return
			}

			localCount := 0
			totalExpected := numProducers * msgsPerProducer

			// Use a deadline to prevent infinite hanging
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				_, err := c.Dequeue()
				if err == nil {
					localCount++
				} else if err == ErrEmptyQueue {
					if localCount >= totalExpected {
						break
					}
					time.Sleep(10 * time.Millisecond)
				} else {
					// Other errors might happen during concurrent GC
					time.Sleep(1 * time.Millisecond)
				}

				if localCount >= totalExpected {
					break
				}
			}

			countMu.Lock()
			counts[cid] = localCount
			countMu.Unlock()
		}(i)
	}

	// 3. Periodic GC and New Consumer injection
	stopGC := make(chan struct{})
	doneGC := make(chan struct{})
	go func() {
		defer close(doneGC)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		newConsumerID := 100
		for {
			select {
			case <-stopGC:
				return
			case <-ticker.C:
				bq.GC()
				// Inject a new consumer middle of the process
				name := fmt.Sprintf("late-c%d", newConsumerID)
				nc, err := bq.NewConsumer(name)
				if err == nil {
					// Just try to read whatever is available
					_, _ = nc.Dequeue()
				}
				newConsumerID++
			}
		}
	}()

	// Ignite!
	close(startSignal)

	// Wait for producers and consumers to finish work
	wg.Wait()
	close(stopGC)
	<-doneGC

	// Final Verification
	for i, c := range counts {
		if c < numProducers*msgsPerProducer {
			t.Errorf("Consumer %d only processed %d/%d messages", i, c, numProducers*msgsPerProducer)
		}
	}

	bq.Close()
}

package bigqueue

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"testing"
)

type benchParam struct {
	arenaSize           int
	arenaSizeString     string
	message             []byte
	messageSizeString   string
	maxInMemArenaCount  int
	maxInMemArenaString string
}

func getBenchParams() []benchParam {
	messageBase := "abcdefghijlkmnopqrstuvwxyzABCDEF"
	// message 128 bytes
	messageBytes := bytes.Repeat([]byte(messageBase), 4)
	// message size is 4KB
	message4KB := bytes.Repeat([]byte(messageBase), 128)
	// message size is 128KB
	message128KB := bytes.Repeat([]byte(messageBase), 4096)
	// message size is 4MB
	message4MB := bytes.Repeat([]byte(messageBase), 131072)

	baseArenaSize := 4 * 1024
	benchParams := []benchParam{
		{baseArenaSize, "4KB", messageBytes, "128B", 3, "12KB"},
		{baseArenaSize, "4KB", messageBytes, "128B", 10, "40KB"},
		{baseArenaSize, "4KB", messageBytes, "128B", 0, "NoLimit"},

		{32 * baseArenaSize, "128KB", message4KB, "4KB", 3, "384KB"},
		{32 * baseArenaSize, "128KB", message4KB, "4KB", 10, "1.25MB"},
		{32 * baseArenaSize, "128KB", message4KB, "4KB", 0, "NoLimit"},

		{1024 * baseArenaSize, "4MB", message128KB, "128KB", 3, "12MB"},
		{1024 * baseArenaSize, "4MB", message128KB, "128KB", 10, "40MB"},
		{1024 * baseArenaSize, "4MB", message128KB, "128KB", 0, "NoLimit"},

		{32 * 1024 * baseArenaSize, "128MB", message4MB, "4MB", 3, "256MB"},
		{32 * 1024 * baseArenaSize, "128MB", message4MB, "4MB", 10, "1.25GB"},
		{32 * 1024 * baseArenaSize, "128MB", message4MB, "4MB", 0, "NoLimit"},
	}

	return benchParams
}

func createBenchDir(b *testing.B, dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0744); err != nil {
			b.Fatalf("unable to create dir for benchmark: %v", err)
		}
	}
}

func removeBenchDir(b *testing.B, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("unable to delete dir for benchmark: %v", err)
	}
}

func BenchmarkNewMmapQueue(b *testing.B) {
	prevValue := -1
	for _, param := range getBenchParams() {
		if prevValue == param.arenaSize {
			continue
		}
		prevValue = param.arenaSize

		b.Run(fmt.Sprintf("ArenaSize-%s", param.arenaSizeString), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				dir := path.Join(os.TempDir(), "testdir")
				createBenchDir(b, path.Join(os.TempDir(), "testdir"))

				b.StartTimer()
				bq, err := NewMmapQueue(dir, SetArenaSize(param.arenaSize),
					SetMaxInMemArenas(param.maxInMemArenaCount))
				if err != nil {
					b.Fatalf("unble to create bigqueue: %v", err)
				}
				b.StopTimer()

				if err := bq.Close(); err != nil {
					b.Fatalf("unable to close bq: %v", err)
				}
				removeBenchDir(b, dir)
			}
		})
	}
}

func BenchmarkEnqueue(b *testing.B) {
	for _, param := range getBenchParams() {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s/MaxMem-%s", param.arenaSizeString,
			param.messageSizeString, param.maxInMemArenaString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)

			bq, err := NewMmapQueue(dir, SetArenaSize(param.arenaSize),
				SetMaxInMemArenas(param.maxInMemArenaCount))
			if err != nil {
				b.Fatalf("unble to create bigqueue: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bq.Enqueue(param.message); err != nil {
					b.Fatalf("unable to enqueue: %v", err)
				}
			}

			b.StopTimer()
			if err := bq.Close(); err != nil {
				b.Fatalf("unable to close bq: %v", err)
			}
			removeBenchDir(b, dir)
		})
	}
}

func BenchmarkEnqueueString(b *testing.B) {
	for _, param := range getBenchParams() {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s/MaxMem-%s", param.arenaSizeString,
			param.messageSizeString, param.maxInMemArenaString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)

			bq, err := NewMmapQueue(dir, SetArenaSize(param.arenaSize),
				SetMaxInMemArenas(param.maxInMemArenaCount))
			if err != nil {
				b.Fatalf("unble to create bigqueue: %v", err)
			}

			message := string(param.message)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bq.EnqueueString(message); err != nil {
					b.Fatalf("unable to enqueue: %v", err)
				}
			}

			b.StopTimer()
			if err := bq.Close(); err != nil {
				b.Fatalf("unable to close bq: %v", err)
			}
			removeBenchDir(b, dir)
		})
	}
}

func BenchmarkDequeue(b *testing.B) {
	for _, param := range getBenchParams() {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s/MaxMem-%s", param.arenaSizeString,
			param.messageSizeString, param.maxInMemArenaString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)

			bq, err := NewMmapQueue(dir, SetArenaSize(param.arenaSize),
				SetMaxInMemArenas(param.maxInMemArenaCount))
			if err != nil {
				b.Fatalf("unble to create bigqueue: %v", err)
			}

			for i := 0; i < b.N; i++ {
				if err := bq.Enqueue(param.message); err != nil {
					b.Fatalf("unable to enqueue: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bq.Dequeue(); err != nil {
					b.Fatalf("unable to dequeue: %v", err)
				}
			}
			b.StopTimer()

			if err := bq.Close(); err != nil {
				b.Fatalf("unable to close bq: %v", err)
			}
			removeBenchDir(b, dir)
		})
	}
}

func BenchmarkPeek(b *testing.B) {
	for _, param := range getBenchParams() {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s/MaxMem-%s", param.arenaSizeString,
			param.messageSizeString, param.maxInMemArenaString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)

			bq, err := NewMmapQueue(dir, SetArenaSize(param.arenaSize),
				SetMaxInMemArenas(param.maxInMemArenaCount))
			if err != nil {
				b.Fatalf("unble to create bigqueue: %v", err)
			}

			if err := bq.Enqueue(param.message); err != nil {
				b.Fatalf("unable to enqueue: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := bq.Peek(); err != nil {
					b.Fatalf("unable to peek: %v", err)
				}
			}

			b.StopTimer()
			if err := bq.Close(); err != nil {
				b.Fatalf("unable to close bq: %v", err)
			}
			removeBenchDir(b, dir)
		})
	}
}

func BenchmarkPeekString(b *testing.B) {
	for _, param := range getBenchParams() {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s/MaxMem-%s", param.arenaSizeString,
			param.messageSizeString, param.maxInMemArenaString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)

			bq, err := NewMmapQueue(dir, SetArenaSize(param.arenaSize),
				SetMaxInMemArenas(param.maxInMemArenaCount))
			if err != nil {
				b.Fatalf("unble to create bigqueue: %v", err)
			}

			if err := bq.Enqueue(param.message); err != nil {
				b.Fatalf("unable to enqueue: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := bq.PeekString(); err != nil {
					b.Fatalf("unable to peek: %v", err)
				}
			}

			b.StopTimer()
			if err := bq.Close(); err != nil {
				b.Fatalf("unable to close bq: %v", err)
			}
			removeBenchDir(b, dir)
		})
	}
}

func BenchmarkParallel(b *testing.B) {
	for _, param := range getBenchParams() {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s/MaxMem-%s", param.arenaSizeString,
			param.messageSizeString, param.maxInMemArenaString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)

			bq, err := NewMmapQueue(dir, SetArenaSize(param.arenaSize),
				SetMaxInMemArenas(param.maxInMemArenaCount))
			if err != nil {
				b.Fatalf("unble to create bigqueue: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			counter := uint64(0)
			b.RunParallel(func(pb *testing.PB) {
				c := atomic.AddUint64(&counter, 1)
				if c%2 != 0 {
					for pb.Next() {
						if err := bq.Enqueue(param.message); err != nil {
							b.Fatalf("unable to enqueue: %v", err)
						}
					}
				} else {
					for pb.Next() {
						for {
							if _, err := bq.Peek(); err == ErrEmptyQueue {
								continue
							} else if err != nil {
								b.Fatalf("unable to peek: %v", err)
							}
							if err := bq.Dequeue(); err == ErrEmptyQueue {
								continue
							} else if err != nil {
								b.Fatalf("unable to dequeue: %v", err)
							} else {
								break
							}
						}
					}
				}
			})

			b.StopTimer()
			if err := bq.Close(); err != nil {
				b.Fatalf("unable to close bq: %v", err)
			}
			removeBenchDir(b, dir)
		})
	}
}

func BenchmarkStringDoubleCopy(b *testing.B) {
	dir := path.Join(os.TempDir(), "testdir")
	createBenchDir(b, dir)
	defer removeBenchDir(b, dir)

	bq, err := NewMmapQueue(dir)
	if err != nil {
		b.Fatalf("error in creating a queue :: %v", err)
	}

	data := "this is a string"
	for i := 0; i < b.N; i++ {
		if err := bq.Enqueue([]byte(data)); err != nil {
			b.Fatalf("error in enqueue :: %v", err)
		}
	}
}

func BenchmarkStringNoCopy(b *testing.B) {
	dir := path.Join(os.TempDir(), "testdir")
	createBenchDir(b, dir)
	defer removeBenchDir(b, dir)

	bq, err := NewMmapQueue(dir)
	if err != nil {
		b.Fatalf("error in creating a queue :: %v", err)
	}

	data := "this is a string"
	for i := 0; i < b.N; i++ {
		if err := bq.EnqueueString(data); err != nil {
			b.Fatalf("error in enqueue :: %v", err)
		}
	}
}

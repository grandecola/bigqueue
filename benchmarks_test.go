package bigqueue

import (
	"bytes"
	"fmt"
	"os"
	"path"
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
	// message size is 16KB
	messageKB := bytes.Repeat([]byte(messageBase), 512)
	// message size is 1MB
	messageMB := bytes.Repeat([]byte(messageBase), 32768)

	baseArenaSize := 4 * 1024
	maxMemorySize := 1 * 1024 * 1024 * 1024 // 1GB

	benchParams := []benchParam{
		{baseArenaSize, "4KB", messageBytes, "128B", 3, "12KB"},
		{baseArenaSize, "4KB", messageBytes, "128B", 10, "40KB"},
		{baseArenaSize, "4KB", messageBytes, "128B", maxMemorySize / baseArenaSize, "1GB"},
		{baseArenaSize, "4KB", messageBytes, "128B", 0, "NoLimit"},

		{baseArenaSize, "4KB", messageKB, "16KB", 10, "40KB"},
		{baseArenaSize, "4KB", messageKB, "16KB", maxMemorySize / baseArenaSize, "1GB"},
		{baseArenaSize, "4KB", messageKB, "16KB", 0, "NoLimit"},

		{baseArenaSize, "4KB", messageMB, "1MB", maxMemorySize / baseArenaSize, "1GB"},
		{baseArenaSize, "4KB", messageMB, "1MB", 0, "NoLimit"},

		{32 * baseArenaSize, "128KB", messageBytes, "128B", 3, "384KB"},
		{32 * baseArenaSize, "128KB", messageBytes, "128B", 10, "1.25MB"},
		{32 * baseArenaSize, "128KB", messageBytes, "128B", maxMemorySize / baseArenaSize / 32, "1GB"},
		{32 * baseArenaSize, "128KB", messageBytes, "128B", 0, "NoLimit"},

		{32 * baseArenaSize, "128KB", messageKB, "16KB", 3, "384KB"},
		{32 * baseArenaSize, "128KB", messageKB, "16KB", 10, "1.25MB"},
		{32 * baseArenaSize, "128KB", messageKB, "16KB", maxMemorySize / baseArenaSize / 32, "1GB"},
		{32 * baseArenaSize, "128KB", messageKB, "16KB", 0, "NoLimit"},

		{32 * baseArenaSize, "128KB", messageMB, "1MB", 3, "384KB"},
		{32 * baseArenaSize, "128KB", messageMB, "1MB", 10, "1.25MB"},
		{32 * baseArenaSize, "128KB", messageMB, "1MB", maxMemorySize / baseArenaSize / 32, "1GB"},
		{32 * baseArenaSize, "128KB", messageMB, "1MB", 0, "NoLimit"},

		{1024 * baseArenaSize, "4MB", messageBytes, "128B", 3, "12MB"},
		{1024 * baseArenaSize, "4MB", messageBytes, "128B", 10, "40MB"},
		{1024 * baseArenaSize, "4MB", messageBytes, "128B", maxMemorySize / baseArenaSize / 1024, "1GB"},
		{1024 * baseArenaSize, "4MB", messageBytes, "128B", 0, "NoLimit"},

		{1024 * baseArenaSize, "4MB", messageKB, "16KB", 3, "12MB"},
		{1024 * baseArenaSize, "4MB", messageKB, "16KB", 10, "40MB"},
		{1024 * baseArenaSize, "4MB", messageKB, "16KB", maxMemorySize / baseArenaSize / 1024, "1GB"},
		{1024 * baseArenaSize, "4MB", messageKB, "16KB", 0, "NoLimit"},

		{1024 * baseArenaSize, "4MB", messageMB, "1MB", 3, "12MB"},
		{1024 * baseArenaSize, "4MB", messageMB, "1MB", 10, "40MB"},
		{1024 * baseArenaSize, "4MB", messageMB, "1MB", maxMemorySize / baseArenaSize / 1024, "1GB"},
		{1024 * baseArenaSize, "4MB", messageMB, "1MB", 0, "NoLimit"},

		{32 * 1024 * baseArenaSize, "128MB", messageBytes, "128B", 3, "256MB"},
		{32 * 1024 * baseArenaSize, "128MB", messageBytes, "128B", 10, "1.25GB"},
		{32 * 1024 * baseArenaSize, "128MB", messageBytes, "128B", 0, "NoLimit"},

		{32 * 1024 * baseArenaSize, "128MB", messageKB, "16KB", 3, "256MB"},
		{32 * 1024 * baseArenaSize, "128MB", messageKB, "16KB", 10, "1.25GB"},
		{32 * 1024 * baseArenaSize, "128MB", messageKB, "16KB", 0, "NoLimit"},

		{32 * 1024 * baseArenaSize, "128MB", messageMB, "1MB", 3, "256MB"},
		{32 * 1024 * baseArenaSize, "128MB", messageMB, "1MB", 10, "1.25GB"},
		{32 * 1024 * baseArenaSize, "128MB", messageMB, "1MB", 0, "NoLimit"},
	}

	return benchParams
}

func createBenchDir(b *testing.B, dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0744); err != nil {
			b.Fatalf("unable to create dir for benchmark: %s", err)
		}
	}
}

func removeBenchDir(b *testing.B, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("unable to delete dir for benchmark: %s", err)
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
					b.Fatalf("unble to create bigqueue: %s", err)
				}
				b.StopTimer()

				_ = bq.Close()
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
				b.Fatalf("unble to create bigqueue: %s", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bq.Enqueue(param.message); err != nil {
					b.Fatalf("unable to enqueue: %s", err)
				}
			}

			b.StopTimer()
			_ = bq.Close()
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
				b.Fatalf("unble to create bigqueue: %s", err)
			}

			for i := 0; i < b.N; i++ {
				if err := bq.Enqueue(param.message); err != nil {
					b.Fatalf("unable to enqueue: %s", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bq.Dequeue(); err != nil {
					b.Fatalf("unable to dequeue: %s", err)
				}
			}
			b.StopTimer()

			_ = bq.Close()
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
				b.Fatalf("unble to create bigqueue: %s", err)
			}

			if err := bq.Enqueue(param.message); err != nil {
				b.Fatalf("unable to enqueue: %s", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := bq.Peek(); err != nil {
					b.Fatalf("unable to peek: %s", err)
				}
			}

			b.StopTimer()
			_ = bq.Close()
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

	data := "aman mangal"
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

	data := "aman mangal"
	for i := 0; i < b.N; i++ {
		if err := bq.EnqueueString(data); err != nil {
			b.Fatalf("error in enqueue :: %v", err)
		}
	}
}

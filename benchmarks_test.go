package bigqueue

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"
)

type benchParam struct {
	arenaSize         int
	arenaSizeString   string
	message           []byte
	messageSizeString string
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

	benchParams := []benchParam{
		{baseArenaSize, "4KB", messageBytes, "128B"},
		{baseArenaSize, "4KB", messageKB, "16KB"},
		{baseArenaSize, "4KB", messageMB, "1MB"},
		{32 * baseArenaSize, "128KB", messageBytes, "128B"},
		{32 * baseArenaSize, "128KB", messageKB, "16KB"},
		{32 * baseArenaSize, "128KB", messageMB, "1MB"},
		{1024 * baseArenaSize, "4MB", messageBytes, "128B"},
		{1024 * baseArenaSize, "4MB", messageKB, "16KB"},
		{1024 * baseArenaSize, "4MB", messageMB, "1MB"},
		{32 * 1024 * baseArenaSize, "128MB", messageBytes, "128B"},
		{32 * 1024 * baseArenaSize, "128MB", messageKB, "16KB"},
		{32 * 1024 * baseArenaSize, "128MB", messageMB, "1MB"},
	}

	return benchParams
}

func createBenchDir(b *testing.B, dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0744); err != nil {
			b.Errorf("unable to create dir for benchmark: %s", err)
		}
	}
}

func removeBenchDir(b *testing.B, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		b.Errorf("unable to delete dir for benchmark: %s", err)
	}
}

func BenchmarkNewBigQueue(b *testing.B) {
	benchParams := getBenchParams()

	for i := 0; i < len(benchParams); i += 3 {
		param := benchParams[i]
		b.Run(fmt.Sprintf("ArenaSize-%s", param.arenaSizeString), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := path.Join(os.TempDir(), "testdir")
				createBenchDir(b, path.Join(os.TempDir(), "testdir"))
				b.StartTimer()
				bq, err := NewBigQueue(dir, SetArenaSize(param.arenaSize))
				if err != nil {
					b.Errorf("unble to create bigqueue: %s", err)
				}
				b.StopTimer()
				bq.Close()
				removeBenchDir(b, dir)
				b.StartTimer()
			}
		})
	}
}

func BenchmarkEnqueue(b *testing.B) {
	benchParams := getBenchParams()

	for _, param := range benchParams {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s", param.arenaSizeString,
			param.messageSizeString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)
			defer removeBenchDir(b, dir)

			bq, err := NewBigQueue(dir, SetArenaSize(param.arenaSize))
			if err != nil {
				b.Errorf("unble to create bigqueue: %s", err)
			}
			defer bq.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bq.Enqueue(param.message); err != nil {
					b.Errorf("unable to enqueue: %s", err)
				}
			}
		})
	}
}

func BenchmarkDequeue(b *testing.B) {
	benchParams := getBenchParams()

	for _, param := range benchParams {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s", param.arenaSizeString,
			param.messageSizeString), func(b *testing.B) {
			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)
			defer removeBenchDir(b, dir)

			bq, err := NewBigQueue(dir, SetArenaSize(param.arenaSize))
			if err != nil {
				b.Errorf("unble to create bigqueue: %s", err)
			}
			defer bq.Close()

			for i := 0; i < b.N; i++ {
				if err := bq.Enqueue(param.message); err != nil {
					b.Errorf("unable to enqueue: %s", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := bq.Dequeue(); err != nil {
					b.Errorf("unable to dequeue: %s", err)
				}
			}
		})
	}
}

func BenchmarkPeek(b *testing.B) {
	benchParams := getBenchParams()

	for _, param := range benchParams {
		b.Run(fmt.Sprintf("ArenaSize-%s/MessageSize-%s", param.arenaSizeString,
			param.messageSizeString), func(b *testing.B) {

			dir := path.Join(os.TempDir(), "testdir")
			createBenchDir(b, dir)
			defer removeBenchDir(b, dir)

			bq, err := NewBigQueue(dir, SetArenaSize(param.arenaSize))
			if err != nil {
				b.Errorf("unble to create bigqueue: %s", err)
			}
			defer bq.Close()

			if err := bq.Enqueue(param.message); err != nil {
				b.Errorf("unable to enqueue: %s", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := bq.Peek(); err != nil {
					b.Errorf("unable to peek: %s", err)
				}
			}
		})
	}
}

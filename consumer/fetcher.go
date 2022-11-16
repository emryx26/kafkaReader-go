package consumer

import (
	"context"
	"fmt"
	"kafkaReader-go/lib"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Fetcher interface {
	Fetch() (hasNext bool)
	Close()
}

type fetcher struct {
	reader    *kafka.Reader
	search    string
	topic     string
	partition int
	cur       int64
	end       int64
}

func NewFetcher(broker string, topic string, partition int, search string, hoursBefore int) Fetcher {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		Partition:      partition,
		CommitInterval: 3600 * time.Second,
	})
	setStartOffset(r, hoursBefore)

	lag, err := r.ReadLag(context.Background())
	if err != nil {
		fmt.Println("connect to kafka err:", err)
		lag = 0
	}

	return &fetcher{
		reader:    r,
		search:    search,
		topic:     topic,
		partition: partition,
		cur:       0,
		end:       lag,
	}
}

func (f *fetcher) Close() {
	if err := f.reader.Close(); err != nil {
		fmt.Printf("failed to close conn %s[%d]: %v\n", f.topic, f.partition, err)
	}
}

func (f *fetcher) Fetch() bool {
	if !f.hasNext() {
		return false
	}

	m, err := f.reader.FetchMessage(context.Background())
	if err != nil {
		fmt.Printf("consume kafka %s[%d] error: %v\n", f.topic, f.partition, err)
		return false
	}

	f.cur++
	s := string(m.Value)
	if len(f.search) == 0 || strings.Contains(s, f.search) {
		fmt.Printf(lib.MsgFmt, lib.SetTimeZone(m.Time), m.Topic, m.Partition, m.Offset, s)
	}

	return f.hasNext()
}

func (f *fetcher) hasNext() bool {
	return f.cur < f.end
}

func setStartOffset(reader *kafka.Reader, hoursBefore int) {
	if hoursBefore > 0 {
		startTime := time.Now().Add(time.Duration(hoursBefore) * -time.Hour)
		reader.SetOffsetAt(context.Background(), startTime)
		return
	}
	reader.SetOffset(kafka.FirstOffset)
}

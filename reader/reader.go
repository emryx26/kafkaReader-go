package reader

import (
	"context"
	"fmt"
	"kafkaReader-go/lib"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Reader interface {
	List()
	Read(sig chan os.Signal)
}

type reader struct {
	broker      string
	topics      []string
	substr      string
	hoursBefore int
	mappings    map[string][]int
}

func NewReader(broker string, topicsString string, substr string, hoursBefore int) Reader {
	topics := lib.Split(topicsString)
	if len(topics) == 0 {
		fmt.Printf("broker:%s\n\n", broker)
	} else {
		st := time.Now().Add(time.Duration(hoursBefore) * -time.Hour)
		fmt.Println("broker:", broker)
		fmt.Println("topics:", topics)
		fmt.Printf("search: '%s'\n", substr)
		fmt.Printf("start at: %v\n\n", lib.SetTimeZone(st))
	}

	return &reader{
		broker:      broker,
		topics:      topics,
		substr:      substr,
		hoursBefore: hoursBefore,
		mappings:    getTopicsPartitionMapping(broker),
	}
}

func (r *reader) List() {
	for k, v := range r.mappings {
		fmt.Println(k, v)
	}
}

func (r *reader) Read(sig chan os.Signal) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var topicDone chan string
	notified := false

	for {
		var signal chan os.Signal
		if !notified {
			signal = sig
		}

		var startTopic <-chan time.Time
		if !notified && topicDone == nil && len(r.topics) > 0 {
			startTopic = time.After(1 * time.Microsecond)
		}

		select {
		case <-signal:
			notified = true
			cancel()

		case <-topicDone:
			topicDone = nil
			r.topics = r.topics[1:]
			if len(r.topics) == 0 || notified {
				return
			}

		case <-startTopic:
			topicDone = make(chan string, 1)
			next := r.topics[0]
			go func() {
				r.readTopic(ctx, next)
				topicDone <- next
			}()
		}
	}
}

func (r *reader) getPartitions(topic string) []int {
	if partitions, ok := r.mappings[topic]; ok {
		return partitions
	}

	fmt.Printf("topic不存在: %s\n", topic)
	return nil
}

func getTopicsPartitionMapping(broker string) map[string][]int {
	conn := getKafkaConn(broker)
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string][]int{}

	for _, p := range partitions {
		if p.Topic == lib.SkipTopic {
			continue
		}
		m[p.Topic] = append(m[p.Topic], p.ID)
	}

	return m
}

func getKafkaConn(broker string) *kafka.Conn {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		panic(err.Error())
	}
	return conn
}

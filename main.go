package main

import (
	"flag"
	"fmt"
	"kafkaReader-go/reader"
	"os"
	"os/signal"
)

const Version = "v1.0.1"

var (
	broker      string
	topic       string
	substr      string
	hoursBefore int
)

func init() {
	fmt.Printf("kafkaReader-Go %s\n", Version)
	flag.StringVar(&broker, "b", "localhost:9092", "kafka broker")
	flag.StringVar(&topic, "t", "", "kafka topic")
	flag.StringVar(&substr, "s", "", "substr to search")
	flag.IntVar(&hoursBefore, "h", 24, "set begin offset based on hours before now, default: 24h before now")
	flag.Parse()
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	reader := reader.NewReader(broker, topic, substr, hoursBefore)
	if len(topic) == 0 {
		reader.List()
		return
	}

	reader.Read(c)
}

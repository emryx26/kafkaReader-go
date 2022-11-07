package reader

import (
	"context"
	"kafkaReader-go/consumer"
)

func (r *reader) readTopic(ctx context.Context, topic string) {
	partitions := r.getPartitions(topic)
	if partitions == nil {
		return
	}

	var subs []consumer.Subscription
	for _, p := range partitions {
		f := consumer.NewFetcher(r.broker, topic, p, r.substr, r.hoursBefore)
		subs = append(subs, consumer.Subscribe(ctx, f))
	}
	wait(consumer.Merge(subs), len(subs))
}

func wait(merge consumer.Subscription, total int) {
	done := 0
	for {
		if done >= total {
			break
		}
		<-merge.Done()
		done++
	}
}

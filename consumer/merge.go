package consumer

type merge struct {
	subs []Subscription
	quit chan struct{}
}

// Merge returns a Subscription that merges the item streams from subs.
// Closing the merged subscription closes subs.
func Merge(subs []Subscription) Subscription {

	m := &merge{
		subs: subs,
		quit: make(chan struct{}),
	}

	go m.listen()
	return m
}

func (m *merge) Done() <-chan struct{} {
	return m.quit
}

func (m *merge) listen() {
	for _, sub := range m.subs {
		go func(s Subscription) {
			<-s.Done()
			m.quit <- struct{}{}
		}(sub)
	}
}

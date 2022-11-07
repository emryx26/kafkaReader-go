package consumer

import (
	"context"
	"time"
)

type Subscription interface {
	Done() <-chan struct{}
}

type sub struct {
	fetcher Fetcher
	done    chan struct{}
}

type fetchResult struct {
	hasNext bool
}

func Subscribe(ctx context.Context, fetcher Fetcher) Subscription {
	s := &sub{
		fetcher: fetcher,
		done:    make(chan struct{}),
	}
	go s.loop(ctx)
	return s
}

func (s *sub) Done() <-chan struct{} {
	return s.done
}

func (s *sub) loop(ctx context.Context) {
	var fetchDone chan fetchResult // if non-nil, Fetch is running
	hasNext := true

	for {
		var done chan struct{}
		if !hasNext {
			s.fetcher.Close()
			done = s.done
		}

		var startFetch <-chan time.Time
		if hasNext && fetchDone == nil {
			startFetch = time.After(1 * time.Microsecond)
		}

		select {
		case done <- struct{}{}:
			return

		case <-ctx.Done():
			if hasNext {
				s.fetcher.Close()
			}
			s.done <- struct{}{}
			return

		case <-startFetch:
			fetchDone = make(chan fetchResult, 1)
			go func() {
				hasNext := s.fetcher.Fetch()
				fetchDone <- fetchResult{hasNext}
			}()

		case result := <-fetchDone:
			fetchDone = nil
			hasNext = result.hasNext
		}
	}
}

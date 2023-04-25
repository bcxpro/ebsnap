package gconc

import (
	"context"
	"fmt"
	"sync"
)

type Transform[I any, O any] func(ctx context.Context, x I) (O, error)

type ParallelOrdered[I any, O any] struct {
	incoming  chan I
	laneCount int
	inLanes   []chan I
	outLanes  []chan O
	outgoing  chan O

	transform     Transform[I, O]
	mergerWg      sync.WaitGroup
	workersWg     sync.WaitGroup
	dispatcherWg  sync.WaitGroup
	workersCtx    context.Context
	workersCancel context.CancelFunc
	done          chan struct{}
	mu            sync.Mutex
	err           error
}

func NewParallelOrdered[I any, O any](ctx context.Context, t Transform[I, O], lanes int, inLaneDepth int, outLaneDepth int) *ParallelOrdered[I, O] {

	in := make(chan I)
	out := make(chan O)
	done := make(chan struct{})

	inLanes := make([]chan I, lanes)
	outLanes := make([]chan O, lanes)

	// lanes are created incoming and outgoing
	for i := 0; i < lanes; i++ {
		inLanes[i] = make(chan I, inLaneDepth)
		outLanes[i] = make(chan O, outLaneDepth)
	}

	workersCtx, workersCancel := context.WithCancel(context.Background())

	f := ParallelOrdered[I, O]{
		incoming:      in,
		outgoing:      out,
		laneCount:     lanes,
		inLanes:       inLanes,
		outLanes:      outLanes,
		transform:     t,
		done:          done,
		workersCtx:    workersCtx,
		workersCancel: workersCancel,
	}

	// Dispatcher
	f.dispatcherWg.Add(1)
	go func() {
		defer f.dispatcherWg.Done()

		// When the dispatcher finishes it closes all the incoming lanes
		defer func() {
			for i := 0; i < f.laneCount; i++ {
				close(f.inLanes[i])
			}
		}()

		var i int
		for wi := range f.incoming {
			// deliver each received work item to the next workers incoming lane
			select {
			case f.inLanes[i] <- wi:
			case <-f.workersCtx.Done():
				return
			case <-ctx.Done():
				f.workersCancel()
				return
			}
			i++
			if i == f.laneCount {
				i = 0
			}
		}
	}()

	// Merger
	f.mergerWg.Add(1)
	go func() {
		defer f.mergerWg.Done()
		defer close(f.outgoing)
		var i int
	Loop:
		for {
			select {
			case wi, ok := <-f.outLanes[i]:
				if !ok {
					break Loop
				}
				f.outgoing <- wi
			case <-ctx.Done():
				break Loop
			}
			i++
			if i == f.laneCount {
				i = 0
			}
		}
	}()

	// Workers. Will start one worker for each lane
	f.workersWg.Add(f.laneCount)
	for i := 0; i < f.laneCount; i++ {
		go func(in <-chan I, out chan<- O, workerId int) {
			defer f.workersWg.Done()
			defer close(out)

		Loop:
			for {
				select {
				case wi, ok := <-in:
					if !ok {
						break Loop
					}
					x, err := f.transform(f.workersCtx, wi)
					if err != nil {
						f.setErr(&ProcessingError[I]{
							Type:     TransformProcessingError,
							WorkItem: wi,
							err:      err,
							message:  "failed transforming work item",
						})
						f.workersCancel()
						break Loop
					}
					select {
					case out <- x:
					case <-f.workersCtx.Done():
						f.setErr(&ProcessingError[I]{
							Type:     DeliveryProcessingError,
							WorkItem: wi,
							err:      workersCtx.Err(),
							message:  "workers context done",
						})
						break Loop
					}
				case <-ctx.Done():
					f.setErr(fmt.Errorf("context done: %w", ctx.Err()))
					f.workersCancel()
					break Loop
				}
			}

		}(f.inLanes[i], f.outLanes[i], i)
	}

	// Finisher
	go func() {
		f.dispatcherWg.Wait()
		f.workersWg.Wait()
		f.mergerWg.Wait()
		close(f.done)
	}()

	return &f
}

func (fan *ParallelOrdered[I, O]) setErr(err error) {
	fan.mu.Lock()
	if fan.err == nil {
		fan.err = err
	}
	fan.mu.Unlock()
}

func (fan *ParallelOrdered[I, O]) In() chan<- I {
	return fan.incoming
}

func (fan *ParallelOrdered[I, O]) Out() <-chan O {
	return fan.outgoing
}

func (fan *ParallelOrdered[I, O]) Done() <-chan struct{} {
	return fan.done
}

func (fan *ParallelOrdered[I, O]) Err() error {
	fan.mu.Lock()
	err := fan.err
	fan.mu.Unlock()
	return err
}

type Generator[T any] struct {
	out  chan T
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func NewGenerator[T any](ctx context.Context, f func(ctx context.Context, out chan<- T) error) *Generator[T] {

	out := make(chan T)
	done := make(chan struct{})
	g := Generator[T]{
		out:  out,
		done: done,
		err:  nil,
	}

	go func() {
		defer close(done)
		defer close(g.out)
		err := f(ctx, g.out)
		g.mu.Lock()
		g.err = err
		g.mu.Unlock()
	}()

	return &g
}

func (g *Generator[T]) Done() <-chan struct{} {
	return g.done
}

func (g *Generator[T]) Err() error {
	g.mu.Lock()
	err := g.err
	g.mu.Unlock()
	return err
}

func (g *Generator[T]) Out() <-chan T {
	return g.out
}

func Contains[T comparable](elems []T, v T) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}
	return false
}

type ProcessingError[T any] struct {
	WorkItem T
	Type     ProcessingErrorType
	message  string
	err      error
}

func (e *ProcessingError[T]) Error() string {
	return e.message
}

func (e *ProcessingError[T]) Unwrap() error {
	return e.err
}

type ProcessingErrorType string

const (
	TransformProcessingError ProcessingErrorType = "Transform"
	DeliveryProcessingError                      = "Delivery"
)

func CopyStringSlice[T ~string](s []T) []string {
	c := make([]string, len(s))
	for i, v := range s {
		c[i] = string(v)
	}
	return c
}

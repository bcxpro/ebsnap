package gconc

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFullCycle_OK(t *testing.T) {

	type out = struct {
		input  int
		output int
	}

	transform := func(ctx context.Context, x int) (out, error) {
		return out{
			input:  x,
			output: x * 2,
		}, nil
	}

	feederCtx, cancel := context.WithCancel(context.Background())

	po := NewParallelOrdered(feederCtx, transform, 2, 0, 0)

	var fedCount int
	var collectedCount int

	// feeder
	feederWg := sync.WaitGroup{}
	feederWg.Add(1)
	go func() {
		defer feederWg.Done()
		defer close(po.In())
	blockLoop:
		for i := 0; i < 1000; i++ {
			select {
			case po.In() <- i:
				fedCount++
			case <-feederCtx.Done():
				// This is not expected to happen
				assert.Fail(t, "Not expected the context to be done")
				break blockLoop
			}
		}
	}()

	// collector
	collectorWg := sync.WaitGroup{}
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		defer cancel()

	myLoop:
		for {
			select {
			case bc, ok := <-po.Out():
				if !ok {
					break myLoop
				}
				assert.EqualValues(t, bc.input*2, bc.output)
				assert.EqualValues(t, collectedCount, bc.input)
				collectedCount++
			case <-po.Done():
				assert.Fail(t, "did not expectet to be done, it should have ended with the last element retrieved")
				break myLoop
			case <-feederCtx.Done():
				assert.Fail(t, "did not expectet to be done, the context was never cancelled at this point")
				break myLoop
			}
		}
		<-po.Done()
		assert.NoError(t, po.Err())
	}()

	collectorWg.Wait()
	feederWg.Wait()
	assert.EqualValues(t, fedCount, collectedCount)
}

// The transform function returns an error when input = 1
func TestFullCycle_ErrorInTransform(t *testing.T) {

	type out = struct {
		input  int
		output int
	}

	errorToEmit := errors.New("we don't like ones")

	transform := func(ctx context.Context, x int) (out, error) {
		if x == 1 {
			return out{}, errorToEmit
		}
		return out{
			input:  x,
			output: x * 2,
		}, nil
	}

	feederCtx := context.Background()
	po := NewParallelOrdered(context.Background(), transform, 2, 0, 0)

	var fedCount int
	var collectedCount int

	// feeder
	feederWg := sync.WaitGroup{}
	feederWg.Add(1)
	go func() {
		defer feederWg.Done()
		defer close(po.In())
		var feederExitedOnDone bool
	blockLoop:
		for i := 0; i < 1000; i++ {
			select {
			case po.In() <- i:
				fedCount++
			case <-po.Done():
				feederExitedOnDone = true
				break blockLoop
			case <-feederCtx.Done():
				// This is not expected to happen
				break blockLoop
			}
		}
		assert.True(t, feederExitedOnDone, "feeder must exit on context Done because there was an error on input 1")
	}()

	// collector
	collectorWg := sync.WaitGroup{}
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()

	myLoop:
		for {
			select {
			case bc, ok := <-po.Out():
				if !ok {
					break myLoop
				}
				assert.EqualValues(t, collectedCount, bc.input)
				collectedCount++
			case <-feederCtx.Done():
				assert.Fail(t, "did not expectet to be done, the context was never cancelled at this point")
				break myLoop
			}
		}
		<-po.Done()
		if assert.Error(t, po.Err(), "must end in an error because input 1 failed") {
			assert.ErrorIs(t, po.Err(), errorToEmit)
		}
	}()

	collectorWg.Wait()
	feederWg.Wait()

}

func TestFullCycle_CancelFeederContext(t *testing.T) {

	type out = struct {
		input  int
		output int
	}

	transform := func(ctx context.Context, x int) (out, error) {
		return out{
			input:  x,
			output: x * 2,
		}, nil
	}

	feederCtx, cancel := context.WithCancel(context.Background())

	po := NewParallelOrdered(feederCtx, transform, 2, 0, 0)

	var fedCount int
	var collectedCount int

	// feeder
	var feederExitOnContextCancel bool
	feederWg := sync.WaitGroup{}
	feederWg.Add(1)
	go func() {
		defer feederWg.Done()
		defer close(po.In())
	blockLoop:
		for i := 0; i < 1000; i++ {
			select {
			case po.In() <- i:
				fedCount++
			case <-feederCtx.Done():
				feederExitOnContextCancel = true
				assert.EqualValues(t, i, 11)
				break blockLoop
			}
			// cancels when i == 10 and continues
			if i == 10 {
				cancel()
			}
		}
		assert.True(t, feederExitOnContextCancel, "expected to end because of context cancelled")
	}()

	// collector
	collectorWg := sync.WaitGroup{}
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		defer cancel()

	myLoop:
		for {
			select {
			case bc, ok := <-po.Out():
				if !ok {
					break myLoop
				}
				assert.EqualValues(t, bc.input*2, bc.output)
				assert.EqualValues(t, collectedCount, bc.input)
				collectedCount++
			case <-po.Done():
				assert.Fail(t, "did not expect to be done, it should have ended with the last element retrieved")
				break myLoop
			}
		}
		<-po.Done()
		if assert.Error(t, po.Err(), "must end in an error because input 1 failed") {
			assert.ErrorIs(t, po.Err(), context.Canceled)
		}
	}()

	collectorWg.Wait()
	feederWg.Wait()
}

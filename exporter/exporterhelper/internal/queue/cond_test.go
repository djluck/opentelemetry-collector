// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitNRegistered blocks until len(c.waiters) == n, polling under mu.
func waitNRegistered(t *testing.T, c *cond, mu sync.Locker, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(c.waiters) == n
	}, time.Second, time.Millisecond)
}

func TestCondSignalWakesExactlyOneWaiter(t *testing.T) {
	mu := &sync.Mutex{}
	c := newCond(mu)

	const numWaiters = 5
	var woken atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numWaiters)
	for range numWaiters {
		go func() {
			defer wg.Done()
			mu.Lock()
			assert.NoError(t, c.Wait(context.Background()))
			woken.Add(1)
			mu.Unlock()
		}()
	}
	waitNRegistered(t, c, mu, numWaiters)

	mu.Lock()
	c.Signal()
	mu.Unlock()

	require.Eventually(t, func() bool { return woken.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, woken.Load())

	mu.Lock()
	c.Broadcast()
	mu.Unlock()
	wg.Wait()
	assert.EqualValues(t, numWaiters, woken.Load())
}

func TestCondBroadcastWakesAllWaiters(t *testing.T) {
	mu := &sync.Mutex{}
	c := newCond(mu)

	const numWaiters = 50
	var wg sync.WaitGroup
	wg.Add(numWaiters)
	for range numWaiters {
		go func() {
			defer wg.Done()
			mu.Lock()
			assert.NoError(t, c.Wait(context.Background()))
			mu.Unlock()
		}()
	}
	waitNRegistered(t, c, mu, numWaiters)

	mu.Lock()
	c.Broadcast()
	mu.Unlock()
	wg.Wait()

	mu.Lock()
	assert.Empty(t, c.waiters)
	mu.Unlock()
}

func TestCondCancelledWaiterIsRemoved(t *testing.T) {
	mu := &sync.Mutex{}
	c := newCond(mu)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		mu.Lock()
		done <- c.Wait(ctx)
		mu.Unlock()
	}()
	waitNRegistered(t, c, mu, 1)
	cancel()

	require.ErrorIs(t, <-done, context.Canceled)
	mu.Lock()
	assert.Empty(t, c.waiters)
	mu.Unlock()
}

// TestCondSignalRacesCancel exercises the race where Signal and ctx cancellation
// fire simultaneously. Signal must never block under the lock, the waiter set
// must not leak, and Wait must return nil or context.Canceled.
func TestCondSignalRacesCancel(t *testing.T) {
	for range 200 {
		mu := &sync.Mutex{}
		c := newCond(mu)
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			mu.Lock()
			done <- c.Wait(ctx)
			mu.Unlock()
		}()
		waitNRegistered(t, c, mu, 1)

		var raceWG sync.WaitGroup
		raceWG.Go(func() {
			mu.Lock()
			c.Signal()
			mu.Unlock()
		})
		raceWG.Go(func() {
			cancel()
		})
		raceWG.Wait()

		if err := <-done; err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
		mu.Lock()
		assert.Empty(t, c.waiters)
		mu.Unlock()
	}
}

// TestCondNoDeadlockUnderLoad reproduces the original deadlock class: many
// Signal/Broadcast calls (all made while holding the lock) interleaved with
// Wait(ctx) calls that frequently cancel. The old buffered-channel
// implementation would hang; this test catches a regression via its deadline.
func TestCondNoDeadlockUnderLoad(t *testing.T) {
	mu := &sync.Mutex{}
	c := newCond(mu)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	const numWaiters, numSignalers = 16, 8
	for range numWaiters {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				mu.Lock()
				_ = c.Wait(ctx)
				mu.Unlock()
				cancel()
			}
		})
	}
	for i := range numSignalers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				if i%2 == 0 {
					c.Signal()
				} else {
					c.Broadcast()
				}
				mu.Unlock()
			}
		})
	}

	time.Sleep(3 * time.Second)
	close(stop)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	budget := 5 * time.Second
	if d, ok := t.Deadline(); ok {
		if rem := time.Until(d) - time.Second; rem < budget {
			budget = rem
		}
	}
	select {
	case <-done:
	case <-time.After(budget):
		buf := make([]byte, 1<<20)
		t.Fatalf("deadlock: goroutines did not finish before deadline\n%s", buf[:runtime.Stack(buf, true)])
	}

	mu.Lock()
	assert.LessOrEqual(t, len(c.waiters), numWaiters)
	mu.Unlock()
}

package circuit_breaker

import (
	"golang.org/x/xerrors"
	"sync"
	"sync/atomic"
	"time"
)

type (
	state interface {
		Failed(breaker innerCircuitBreaker)
	}

	stateOpen     struct{}
	stateHalfOpen struct{}
	stateClosed   struct{}

	CircuitBreaker interface {
		FailureCount() uint64
		FailureCountResetTimeout() time.Duration
		InvokeFunc(func() error) error
	}

	innerCircuitBreaker interface {
		CircuitBreaker
		IsThresholdExceeded() bool
		IsOpen() bool
		IsClosed() bool
		Open()
		IncrementFailureCount()
		DecrementFailureCount()
	}

	CircuitBreakerImpl struct {
		threshold          uint64
		failureCount       uint64
		state              state
		stateMutationMutex *sync.Mutex

		halfOpenTimeout          time.Duration
		failureCountResetTimeout time.Duration
	}
)

var ErrCircuitBreakerOpen = xerrors.New("circuit breaker is open. canceled invoke function")

func NewCircuitBreaker(
	threshold uint64,
	halfOpenTimeout time.Duration,
	failureCountResetTimeout time.Duration,
) CircuitBreaker {
	return &CircuitBreakerImpl{
		threshold:                threshold,
		failureCount:             0,
		state:                    Closed,
		stateMutationMutex:       &sync.Mutex{},
		halfOpenTimeout:          halfOpenTimeout,
		failureCountResetTimeout: failureCountResetTimeout,
	}
}

var (
	Open     = stateOpen{}
	HalfOpen = stateHalfOpen{}
	Closed   = stateClosed{}
)

func (s stateClosed) Failed(breaker innerCircuitBreaker) {
	breaker.IncrementFailureCount()
	if breaker.IsThresholdExceeded() {
		breaker.Open()
	}

	go func() {
		time.Sleep(breaker.FailureCountResetTimeout())
		if breaker.IsClosed() {
			breaker.DecrementFailureCount()
		}
	}()
}

func (s stateHalfOpen) Failed(breaker innerCircuitBreaker) {
	breaker.Open()
}

func (s stateOpen) Failed(breaker innerCircuitBreaker) {
	// DO NOT Nothing. if this method was called, the Circuit Breaker has a bug
}

func (c *CircuitBreakerImpl) InvokeFunc(f func() error) error {
	if c.IsOpen() {
		return ErrCircuitBreakerOpen
	}
	result := f()
	if result != nil {
		c.NotifyFailure()
		return result
	}
	c.NotifySuccess()
	return nil
}

func (c *CircuitBreakerImpl) IncrementFailureCount() {
	atomic.AddUint64(&c.failureCount, 1)
}

func (c *CircuitBreakerImpl) DecrementFailureCount() {
	atomic.AddUint64(&c.failureCount, ^0)
}

func (c *CircuitBreakerImpl) resetFailureCount() {
	_ = atomic.SwapUint64(&c.failureCount, 0)
}

func (c *CircuitBreakerImpl) NotifyFailure() {
	c.state.Failed(c)
}

func (c *CircuitBreakerImpl) NotifySuccess() {
	if c.IsHalfOpen() {
		c.Close()
	}
}

func (c *CircuitBreakerImpl) IsThresholdExceeded() bool {
	return c.FailureCount() >= c.threshold
}

func (c *CircuitBreakerImpl) FailureCount() uint64 {
	return atomic.LoadUint64(&c.failureCount)
}

func (c *CircuitBreakerImpl) FailureCountResetTimeout() time.Duration {
	return c.failureCountResetTimeout
}

func (c *CircuitBreakerImpl) mutateState(state state) {
	c.stateMutationMutex.Lock()
	defer c.stateMutationMutex.Unlock()
	c.state = state
}

func (c *CircuitBreakerImpl) Open() {
	c.mutateState(Open)
	go func() {
		time.Sleep(c.halfOpenTimeout)
		if c.IsOpen() {
			c.HalfOpen()
		}
	}()
}

func (c *CircuitBreakerImpl) HalfOpen() {
	c.mutateState(HalfOpen)
}

func (c *CircuitBreakerImpl) Close() {
	c.mutateState(Closed)
	c.resetFailureCount()
}

func (c *CircuitBreakerImpl) IsOpen() bool {
	return c.state == Open
}

func (c *CircuitBreakerImpl) IsHalfOpen() bool {
	return c.state == HalfOpen
}

func (c *CircuitBreakerImpl) IsClosed() bool {
	return c.state == Closed
}

package circuit_breaker

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/xerrors"
)

type (
	state interface {
		onFailed(breaker innerCircuitBreaker)
		fmt.Stringer
		fmt.GoStringer
	}

	stateOpen     struct{}
	stateHalfOpen struct{}
	stateClosed   struct {
		failureCount uint64
		threshold    uint64
	}

	CircuitBreaker interface {
		FailureCountResetTimeout() time.Duration
		HalfOpenTimeout() time.Duration
		InvokeFunc(func() error) error
		View() CircuitBreakerView
		IsOpen() bool
	}

	IgnoreError struct {
		err error
	}

	innerCircuitBreaker interface {
		CircuitBreaker
		Open()
	}

	CircuitBreakerImpl struct {
		threshold          uint64
		state              state
		stateMutationMutex *sync.Mutex

		halfOpenTimeout          time.Duration
		failureCountResetTimeout time.Duration

		logWriter io.StringWriter
	}

	CircuitBreakerView struct {
		Threshold                uint64        `json:"threshold"`
		State                    string        `json:"state"`
		HalfOpenTimeout          time.Duration `json:"half_open_timeout"`
		FailureCountResetTimeout time.Duration `json:"failure_count_reset_timeout"`
	}

	option func(i *CircuitBreakerImpl)
)

func isIgnoreError(err error) bool {
	_, ok := err.(*IgnoreError)
	return ok
}

func NewIgnoreError(err error) *IgnoreError {
	return &IgnoreError{err: err}
}

func (e *IgnoreError) Error() string {
	return fmt.Sprintf("circuit_breaker ignore error: %s", e.err.Error())
}

func (e *IgnoreError) Unwrap() error {
	return e.err
}

var (
	SetLogger = func(logger io.StringWriter) option {
		return func(i *CircuitBreakerImpl) {
			i.logWriter = logger
		}
	}
)

func (c *CircuitBreakerImpl) View() CircuitBreakerView {
	return CircuitBreakerView{
		Threshold:                c.threshold,
		State:                    c.state.String(),
		HalfOpenTimeout:          c.halfOpenTimeout,
		FailureCountResetTimeout: c.failureCountResetTimeout,
	}
}
func (s stateClosed) GoString() string {
	return s.String()
}

func (s stateHalfOpen) GoString() string {
	return s.String()
}

func (s stateOpen) GoString() string {
	return s.String()
}

func (s stateClosed) String() string {
	return fmt.Sprintf("closed: {failureCount: %d, threshold: %d}", s.failureCount, s.threshold)
}

func (s stateHalfOpen) String() string {
	return "halfOpen"
}

func (s stateOpen) String() string {
	return "open"
}

var ErrCircuitBreakerOpen = xerrors.New("circuit breaker is open. canceled invoke function")

var _ CircuitBreaker = (*CircuitBreakerImpl)(nil)

func NewCircuitBreaker(
	threshold uint64,
	halfOpenTimeout time.Duration,
	failureCountResetTimeout time.Duration,
	options ...option,
) CircuitBreaker {
	impl := &CircuitBreakerImpl{
		threshold:                threshold,
		state:                    closed(threshold),
		stateMutationMutex:       &sync.Mutex{},
		halfOpenTimeout:          halfOpenTimeout,
		failureCountResetTimeout: failureCountResetTimeout,
	}

	for _, apply := range options {
		apply(impl)
	}

	return impl
}

var (
	open     = stateOpen{}
	halfOpen = stateHalfOpen{}
)

func closed(threshold uint64) *stateClosed {
	return &stateClosed{
		failureCount: 0,
		threshold:    threshold,
	}
}

func (s *stateClosed) onFailed(breaker innerCircuitBreaker) {
	s.IncrementFailureCount()
	if s.IsThresholdExceeded() {
		breaker.Open()
	}

	go func() {
		time.Sleep(breaker.FailureCountResetTimeout())
		s.DecrementFailureCount()
	}()
}

func (s *stateClosed) IncrementFailureCount() {
	atomic.AddUint64(&s.failureCount, 1)
}

func (s *stateClosed) DecrementFailureCount() {
	atomic.AddUint64(&s.failureCount, ^uint64(0))
}

func (s stateHalfOpen) onFailed(breaker innerCircuitBreaker) {
	breaker.Open()
}

func (s stateOpen) onFailed(breaker innerCircuitBreaker) {
	// DO NOT Nothing. if this method was called, the Circuit Breaker has a bug
}

func (c *CircuitBreakerImpl) log(format string, args ...interface{}) {
	if c.logWriter != nil {
		_, _ = c.logWriter.WriteString(fmt.Sprintf(format, args...))
	}
}

func (c *CircuitBreakerImpl) InvokeFunc(f func() error) error {
	if c.IsOpen() {
		return ErrCircuitBreakerOpen
	}
	err := f()
	if err != nil && !isIgnoreError(err) {
		c.NotifyFailure()
		return err
	}
	c.NotifySuccess()
	return nil
}

func (c *CircuitBreakerImpl) NotifyFailure() {
	c.state.onFailed(c)
}

func (c *CircuitBreakerImpl) NotifySuccess() {
	if c.isHalfOpen() {
		c.log("invoke func succeeded. \n")
		c.Close()
	}
}

func (s *stateClosed) IsThresholdExceeded() bool {
	return s.FailureCount() >= s.threshold
}

func (s *stateClosed) FailureCount() uint64 {
	return atomic.LoadUint64(&s.failureCount)
}

func (c *CircuitBreakerImpl) HalfOpenTimeout() time.Duration {
	return c.halfOpenTimeout
}

func (c *CircuitBreakerImpl) FailureCountResetTimeout() time.Duration {
	return c.failureCountResetTimeout
}

func (c *CircuitBreakerImpl) mutateState(state state) {
	c.log("state transition %#v -> %#v\n", c.state, state)
	c.stateMutationMutex.Lock()
	defer c.stateMutationMutex.Unlock()
	c.state = state
}

func (c *CircuitBreakerImpl) Open() {
	c.mutateState(open)
	go func() {
		time.Sleep(c.halfOpenTimeout)
		if c.IsOpen() {
			c.log("halfOpenTimeout exceeded.\n")
			c.HalfOpen()
		}
	}()
}

func (c *CircuitBreakerImpl) HalfOpen() {
	c.mutateState(halfOpen)
}

func (c *CircuitBreakerImpl) Close() {
	c.mutateState(closed(c.threshold))
}

func (c *CircuitBreakerImpl) IsOpen() bool {
	return c.state == open
}

func (c *CircuitBreakerImpl) isHalfOpen() bool {
	return c.state == halfOpen
}

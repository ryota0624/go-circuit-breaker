package circuit_breaker

import (
	"fmt"
	"sync"
	"time"
)

type (
	KeyCompatible = fmt.Stringer

	// Key
	Key string

	circuitBreakerMap struct {
		sMap *sync.Map
	}

	PartitionedCircuitBreaker interface {
		InvokeFunc(KeyCompatible, func() error) error
	}

	PartitionedCircuitBreakerImpl struct {
		threshold                uint64
		halfOpenTimeout          time.Duration
		failureCountResetTimeout time.Duration

		circuitBreakerMap *circuitBreakerMap
	}
)

func NewKey(st fmt.Stringer) Key {
	return Key(st.String())
}

func (m *circuitBreakerMap) Load(key Key) (CircuitBreaker, bool) {
	v, ok := m.sMap.Load(key)
	if ok {
		return v.(CircuitBreaker), true
	}

	return nil, false
}

func (m *circuitBreakerMap) Store(key Key, breaker CircuitBreaker) {
	m.sMap.Store(key, breaker)
}

func (p PartitionedCircuitBreakerImpl) InvokeFunc(k KeyCompatible, f func() error) error {
	key := NewKey(k)
	breaker, ok := p.circuitBreakerMap.Load(key)
	if !ok {
		breaker = NewCircuitBreaker(p.threshold, p.halfOpenTimeout, p.failureCountResetTimeout)
	}
	result := breaker.InvokeFunc(f)
	p.circuitBreakerMap.Store(key, breaker)
	return result
}

var _ PartitionedCircuitBreaker = (*PartitionedCircuitBreakerImpl)(nil)

func NewPartitionedCircuitBreaker(
	threshold uint64,
	halfOpenTimeout time.Duration,
	failureCountResetTimeout time.Duration,
) PartitionedCircuitBreaker {
	cMap := &circuitBreakerMap{
		sMap: &sync.Map{},
	}

	return &PartitionedCircuitBreakerImpl{
		threshold:                threshold,
		halfOpenTimeout:          halfOpenTimeout,
		failureCountResetTimeout: failureCountResetTimeout,
		circuitBreakerMap:        cMap,
	}
}

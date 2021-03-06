package circuit_breaker

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/xerrors"
)

type (
	TestingLogger struct {
		t *testing.T
	}
)

func NewTestingLogger(t *testing.T) *TestingLogger {
	return &TestingLogger{t: t}
}

func (t TestingLogger) WriteString(s string) (n int, err error) {
	t.t.Logf(s)
	return 0, nil
}

func FailFunc() error {
	return xerrors.New("fail")
}

func SuccessFunc() error {
	return nil
}

func TestCircuitBreakerImpl_InvokeFunc(t *testing.T) {
	type fields struct {
		threshold                uint64
		state                    state
		halfOpenTimeout          time.Duration
		failureCountResetTimeout time.Duration
	}
	type args struct {
		f func() error
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    CircuitBreakerImpl
	}{
		{
			name: "Close時に引数の関数が失敗した場合failureCountがincrementされる。",
			fields: fields{
				threshold: 5,
				state: &stateClosed{
					failureCount: 0,
					threshold:    5,
				},
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: FailFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				threshold: 5,
				state: &stateClosed{
					failureCount: 1,
					threshold:    5,
				},
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
		{
			name: "関数の実行後にfailureCountがthresholdと同じ場合stateがOpenになる",
			fields: fields{
				threshold: 5,
				state: &stateClosed{
					failureCount: 4,
					threshold:    5,
				},
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: FailFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				threshold:                5,
				state:                    open,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
		{
			name: "Openだった場合関数は必ず失敗する",
			fields: fields{
				state: open,
			},
			args: args{
				f: SuccessFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				state: open,
			},
		},
		{
			name: "HalfOpen時に引数の関数が成功した場合Closeになり、failureCountがリセットされる",
			fields: fields{
				threshold:                5,
				state:                    halfOpen,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: SuccessFunc,
			},
			wantErr: false,
			want: CircuitBreakerImpl{
				threshold:                5,
				state:                    closed(5),
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
		{
			name: "HalfOpen時に引数の関数が失敗した場合Openになる",
			fields: fields{
				threshold:                5,
				state:                    halfOpen,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: FailFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				threshold:                5,
				state:                    open,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mutex = &sync.Mutex{}

			c := &CircuitBreakerImpl{
				threshold:                tt.fields.threshold,
				state:                    tt.fields.state,
				stateMutationMutex:       mutex,
				halfOpenTimeout:          tt.fields.halfOpenTimeout,
				failureCountResetTimeout: tt.fields.failureCountResetTimeout,
				logWriter:                NewTestingLogger(t),
			}

			tt.want.stateMutationMutex = mutex
			if err := c.InvokeFunc(tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("InvokeFunc() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.want.View() != c.View() {
				t.Errorf("unexpected circuit_breaker actual = %#v want = %#v", c.View(), tt.want.View())
			}
		})
	}
}

func TestCircuitBreaker_InvokeFunc_Scenario(t *testing.T) {
	circuitBreaker := NewCircuitBreaker(3, time.Second*3, time.Second, SetLogger(NewTestingLogger(t)))

	invokeUntilOpen := func() {
		_ = circuitBreaker.InvokeFunc(FailFunc)
		_ = circuitBreaker.InvokeFunc(FailFunc)
		_ = circuitBreaker.InvokeFunc(FailFunc)
		if !circuitBreaker.IsOpen() {
			t.Errorf("unexpected state. IsOpen() should return true.  %#v", circuitBreaker)
			return
		}
	}

	_ = circuitBreaker.InvokeFunc(SuccessFunc)
	_ = circuitBreaker.InvokeFunc(SuccessFunc)
	_ = circuitBreaker.InvokeFunc(SuccessFunc)
	_ = circuitBreaker.InvokeFunc(SuccessFunc)
	if circuitBreaker.IsOpen() {
		t.Errorf("unexpected state. IsOpen() should return false. %#v", circuitBreaker)
		return
	}

	invokeUntilOpen()

	time.Sleep(circuitBreaker.HalfOpenTimeout() + time.Second)
	_ = circuitBreaker.InvokeFunc(SuccessFunc)
	if circuitBreaker.IsOpen() {
		t.Errorf("unexpected state. IsOpen() should return false. %#v", circuitBreaker)
		return
	}
	invokeUntilOpen()

	time.Sleep(circuitBreaker.HalfOpenTimeout() + time.Second)
	_ = circuitBreaker.InvokeFunc(FailFunc)
	if !circuitBreaker.IsOpen() {
		t.Errorf("unexpected state. IsOpen() should return true.  %#v", circuitBreaker)
		return
	}

	err := circuitBreaker.InvokeFunc(FailFunc)

	if !xerrors.Is(err, ErrCircuitBreakerOpen) {
		t.Errorf("unexpected error occurred. %+v", err)
		return
	}

}

func TestCircuitBreaker_InvokeFunc_Parallel(t *testing.T) {
	threshold := uint64(5)
	circuitBreaker := NewCircuitBreaker(threshold, time.Second*3, time.Second, SetLogger(NewTestingLogger(t)))
	wg := &sync.WaitGroup{}
	for range make([]int, threshold) {
		wg.Add(1)
		go func() {
			_ = circuitBreaker.InvokeFunc(FailFunc)
			wg.Done()
		}()
	}

	wg.Wait()
	if !circuitBreaker.IsOpen() {
		t.Errorf("unexpected state. IsOpen() should return true.  %#v", circuitBreaker)
		return
	}
}

func TestCircuitBreakerImpl_Open(t *testing.T) {
	type fields struct {
		threshold                uint64
		state                    state
		halfOpenTimeout          time.Duration
		failureCountResetTimeout time.Duration
	}
	tests := []struct {
		name         string
		fields       fields
		want         CircuitBreakerImpl
		interception func(*CircuitBreakerImpl)
	}{
		{
			name: "Open後halfOpenTimeout分経過し、未だOpenだった場合HalfOpenになる",
			fields: fields{
				state:           closed(1),
				halfOpenTimeout: time.Second,
			},
			want: CircuitBreakerImpl{
				state:           halfOpen,
				halfOpenTimeout: time.Second,
			},
		},
		{
			name: "Open後halfOpenTimeout分経過し、すでにCloseだった場合なにもおきない",
			fields: fields{
				threshold:       1,
				state:           closed(1),
				halfOpenTimeout: time.Second,
			},
			want: CircuitBreakerImpl{
				threshold:       1,
				state:           closed(1),
				halfOpenTimeout: time.Second,
			},
			interception: func(impl *CircuitBreakerImpl) {
				impl.state = closed(1)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mutex = &sync.Mutex{}
			c := &CircuitBreakerImpl{
				threshold:                tt.fields.threshold,
				state:                    tt.fields.state,
				stateMutationMutex:       mutex,
				halfOpenTimeout:          tt.fields.halfOpenTimeout,
				failureCountResetTimeout: tt.fields.failureCountResetTimeout,
				logWriter:                NewTestingLogger(t),
			}

			c.Open()
			tt.want.stateMutationMutex = mutex
			if tt.interception != nil {
				tt.interception(c)
			}
			time.Sleep(c.halfOpenTimeout + time.Second)
			if tt.want.View() != c.View() {
				t.Errorf("unexpected circuit_breaker actual = %#v want = %#v", c.View(), tt.want.View())
			}
		})
	}
}

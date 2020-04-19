package circuit_breaker

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/xerrors"
)

func FailFunc() error {
	return xerrors.New("fail")
}

func SuccessFunc() error {
	return nil
}

func TestCircuitBreakerImpl_InvokeFunc(t *testing.T) {
	type fields struct {
		threshold                uint64
		failureCount             uint64
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
				threshold:                5,
				failureCount:             0,
				state:                    Closed,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: FailFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				threshold:                5,
				failureCount:             1,
				state:                    Closed,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
		{
			name: "関数の実行後にfailureCountがthresholdと同じ場合stateがOpenになる",
			fields: fields{
				threshold:                5,
				failureCount:             4,
				state:                    Closed,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: FailFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				threshold:                5,
				failureCount:             5,
				state:                    Open,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
		{
			name: "Openだった場合関数は必ず失敗する",
			fields: fields{
				threshold:                5,
				failureCount:             5,
				state:                    Open,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: SuccessFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				threshold:                5,
				failureCount:             5,
				state:                    Open,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
		{
			name: "HalfOpen時に引数の関数が成功した場合Closeになり、failureCountがリセットされる",
			fields: fields{
				threshold:                5,
				failureCount:             5,
				state:                    HalfOpen,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: SuccessFunc,
			},
			wantErr: false,
			want: CircuitBreakerImpl{
				threshold:                5,
				failureCount:             0,
				state:                    Closed,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
		},
		{
			name: "HalfOpen時に引数の関数が失敗した場合Closeになる",
			fields: fields{
				threshold:                5,
				failureCount:             5,
				state:                    HalfOpen,
				halfOpenTimeout:          time.Hour,
				failureCountResetTimeout: time.Hour,
			},
			args: args{
				f: FailFunc,
			},
			wantErr: true,
			want: CircuitBreakerImpl{
				threshold:                5,
				failureCount:             5,
				state:                    Open,
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
				failureCount:             tt.fields.failureCount,
				state:                    tt.fields.state,
				stateMutationMutex:       mutex,
				halfOpenTimeout:          tt.fields.halfOpenTimeout,
				failureCountResetTimeout: tt.fields.failureCountResetTimeout,
			}

			tt.want.stateMutationMutex = mutex
			if err := c.InvokeFunc(tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("InvokeFunc() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.want != *c {
				t.Errorf("unexpected circuit_breaker actual = %#v want = %#v", *c, tt.want)
			}
		})
	}
}

func TestCircuitBreakerImpl_Open(t *testing.T) {
	type fields struct {
		threshold                uint64
		failureCount             uint64
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
				state:           Closed,
				halfOpenTimeout: time.Second,
			},
			want: CircuitBreakerImpl{
				state:           HalfOpen,
				halfOpenTimeout: time.Second,
			},
		},
		{
			name: "Open後halfOpenTimeout分経過し、すでにCloseだった場合なにもおきない",
			fields: fields{
				state:           Closed,
				halfOpenTimeout: time.Second,
			},
			want: CircuitBreakerImpl{
				state:           Closed,
				halfOpenTimeout: time.Second,
			},
			interception: func(impl *CircuitBreakerImpl) {
				impl.state = Closed
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mutex = &sync.Mutex{}
			c := &CircuitBreakerImpl{
				threshold:                tt.fields.threshold,
				failureCount:             tt.fields.failureCount,
				state:                    tt.fields.state,
				stateMutationMutex:       mutex,
				halfOpenTimeout:          tt.fields.halfOpenTimeout,
				failureCountResetTimeout: tt.fields.failureCountResetTimeout,
			}

			c.Open()
			tt.want.stateMutationMutex = mutex
			if tt.interception != nil {
				tt.interception(c)
			}
			time.Sleep(c.halfOpenTimeout + time.Second)
			if tt.want != *c {
				t.Errorf("unexpected circuit_breaker actual = %#v want = %#v", *c, tt.want)
			}
		})
	}
}

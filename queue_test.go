package gokcqueue

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type d struct {
	s string
}

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func Benchmark_foo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testData := []string{"foo", "bar", "bar", "baz", "bop"}
		ctx, cancel := context.WithCancel(context.Background())
		q := New(ctx)

		for i, s := range testData {
			q.Add(s, time.Now().Add(time.Duration(i)*time.Second), d{s: s})
		}

		for e := range q.Get() {
			r, ok := e.(d)
			if !ok {
				b.Logf("Got bad type from event!")
			} else {
				b.Logf("Got data from event: %s", r.s)
				if r.s == "bop" {
					break
				}
			}
		}

		cancel()
	}
}

func Test_foo(t *testing.T) {
	t.Run("foo", func(t *testing.T) {
		want := []string{"foo", "bar", "zoop", "baz2", "bop2"}
		got := []string{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		q := New(ctx)

		go func() {
			for e := range q.Get() {
				r, ok := e.(d)
				if !ok {
					t.Logf("Got bad type from event!")
				} else {
					t.Logf("Got data from event: %s", r.s)
					got = append(got, r.s)
				}
			}
		}()

		q.Add("zoop", time.Now().Add(time.Second*2), d{s: "zoop"})
		q.Add("foo", time.Now().Add(time.Second), d{s: "foo"})
		q.Add("bar", time.Now().Add(time.Second), d{s: "bar"})

		<-time.After(3 * time.Second)

		q.Add("baz", time.Now().Add(time.Second), d{s: "baz1"})
		q.Add("baz", time.Now().Add(time.Second), d{s: "baz2"})

		q.Add("bop", time.Now().Add(time.Second), d{s: "bop1"})
		q.Remove("bop")
		q.Add("bop", time.Now().Add(time.Second), d{s: "bop2"})

		<-time.After(2 * time.Second)
		assert.Equal(t, want, got, "The result list should be in the correct order.")
	})
}

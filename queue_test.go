package gokcqueue

import (
	"testing"
	"time"
)

type d struct {
	s string
}

func Test_foo(t *testing.T) {
	t.Run("foo", func(t *testing.T) {
		q := KeyedCalendarQueue{}
		w := time.Now().Add(time.Second)
		q.Add("foo", w, d{s: "foo"})

	})
}

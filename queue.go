package gokcqueue

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const initialBucketSize = 16

type entry struct {
	key  string
	when time.Time
	data interface{}
}

type entryList []*entry

type KeyedCalendarQueue struct {
	interrupt chan time.Time
	bucketFor map[string]int
	buckets   []entryList
	data      chan interface{}
	cancel    context.CancelFunc
	lock      sync.Mutex
	closed    bool
}

func New(ctx context.Context) *KeyedCalendarQueue {
	q := &KeyedCalendarQueue{}
	q.start(ctx)
	return q
}

func (q *KeyedCalendarQueue) Add(key string, when time.Time, data interface{}) (interface{}, bool, error) {
	logrus.Debugf("Add: key=%s when=%v, data=%v", key, when, data)

	if q.closed {
		return nil, false, errors.New("closed")
	}
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.buckets == nil {
		q.buckets = make([]entryList, initialBucketSize)
	}

	if q.bucketFor == nil {
		q.bucketFor = map[string]int{}
	}

	// if an entry with this key already exists, remove the current one
	existing, ok := q.removeByKey(key)
	if !ok {
		existing = &entry{}
	}

	created := &entry{
		key:  key,
		when: when,
		data: data,
	}

	q.insert(created)
	q.sendInterrupt(when)
	return existing.data, ok, nil
}

func (q *KeyedCalendarQueue) Remove(key string) (time.Time, interface{}, error) {
	logrus.Debugf("Remove: key=%s", key)
	if q.closed {
		return time.Time{}, nil, errors.New("closed")
	}
	q.lock.Lock()
	defer q.lock.Unlock()

	if e, ok := q.removeByKey(key); ok {
		return e.when, e.data, nil
	}

	return time.Time{}, nil, errors.New("key not found")
}

func (q *KeyedCalendarQueue) Get() <-chan interface{} {
	if q.data == nil {
		q.start(context.Background())
	}
	return q.data
}

func (q *KeyedCalendarQueue) Close() {
	if q.cancel != nil {
		q.cancel()
	}
}

func (q *KeyedCalendarQueue) start(ctx context.Context) {
	ctxInner, cancel := context.WithCancel(context.Background())
	q.lock.Lock()
	defer q.lock.Unlock()

	q.data = make(chan interface{}, 1)
	q.interrupt = make(chan time.Time, 1)
	q.cancel = cancel
	go q.timerLoop(ctxInner)
}

func (q *KeyedCalendarQueue) sendInterrupt(t time.Time) {
	if q.interrupt != nil {
		q.interrupt <- t
		runtime.Gosched()
	}
}

func (q *KeyedCalendarQueue) timerLoop(ctx context.Context) {
	var c, f bool
	var n time.Time
	var t *time.Timer = time.NewTimer(0)

	for {
		// if we have changed the next timer scheduling and it is non-zero, reset the timer.
		if c && !n.IsZero() {
			// stop the timer; drain the channel if necessary
			if !t.Stop() && !f {
				<-t.C
			}
			t.Reset(time.Until(n))
			c = false
		}

		select {
		case <-t.C:
			f = true
			for {
				q.lock.Lock()
				e := q.getNextEntry(n)
				if e == nil {
					// no more entries; wait for interrupt
					q.lock.Unlock()
					n = time.Time{}
					break
				} else if time.Until(e.when) > 0 {
					// entry is not due yet, go back to waiting
					q.lock.Unlock()
					n = e.when
					c = true
					break
				} else {
					// entry is due, remove and return it
					q.remove(e)
					q.lock.Unlock()
					q.data <- e.data
				}
			}
		case i := <-q.interrupt:
			if n.IsZero() || i.Before(n) {
				// if the timer wasn't going to fire, or if the new entry is due before the timer would fire, reschedule
				n = i
				c = true
			}
		case <-ctx.Done():
			// context cancelled - close the channel and remove data
			q.lock.Lock()
			close(q.data)
			q.buckets = nil
			q.bucketFor = nil
			q.closed = true
			q.lock.Unlock()
			return
		}
	}
}

func (q *KeyedCalendarQueue) getNextEntry(t time.Time) *entry {
	b := q.findBucket(t)

	// See if the next entry in the current bucket is from the same epoch
	if e := q.peek(b); e != nil && q.sameEpoch(t, e.when) {
		return e
	}

	// Step through the next round of epochs looking for entries in their buckets, also noting the earliest entry we find
	var n *entry
	l := len(q.buckets)
	for i := 1; i < l; i++ {
		t = t.Add(time.Second * time.Duration(l))
		b = (b + 1) % l
		e := q.peek(b)
		if e != nil {
			if q.sameEpoch(t, e.when) {
				return e
			}
			if n == nil || e.when.Before(n.when) {
				n = e
			}
		}
	}

	// TODO: add support for resizing the buckets based on distance between entries

	// If we made it through all the buckets without finding an event in any of the epochs, jump forward to the next earliest event.
	// If we didn't find an event at all then all the buckets are empty, which is fine.
	return n
}

func (q *KeyedCalendarQueue) findBucket(t time.Time) int {
	l := int64(len(q.buckets))
	if l == 0 {
		return 0
	}
	return int((t.Unix() / l) % l)
}

func (q *KeyedCalendarQueue) sameEpoch(a time.Time, b time.Time) bool {
	l := int64(len(q.buckets))
	if l == 0 {
		return false
	}
	return int(a.Unix()/l) == int(b.Unix()/l)
}

func (q *KeyedCalendarQueue) insert(n *entry) {
	b := q.findBucket(n.when)
	if !(len(q.buckets) > b) {
		return
	}

	l := q.buckets[b]
	p := len(l) - 1

	if p < 0 || n.when.After(l[p].when) {
		// if the current list is empty, or if the new entry is after the last entry, just append
		q.bucketFor[n.key] = b
		q.buckets[b] = append(l, n)
	} else {
		// find the first entry that we should insert before
		for i, e := range l {
			if n.when.Before(e.when) {
				p = i
				break
			}
		}
		l = append(l[:p+1], l[p:]...)
		l[p] = n
		q.buckets[b] = l
	}

	q.bucketFor[n.key] = b
}

func (q *KeyedCalendarQueue) remove(n *entry) bool {
	b := q.findBucket(n.when)
	if !(len(q.buckets) > b) {
		return false
	}

	l := q.buckets[b]
	last := len(l)
	for i, e := range l {
		if e == n {
			if i == last {
				q.buckets[b] = l[:i]
				delete(q.bucketFor, n.key)
				return true
			} else {
				q.buckets[b] = append(l[:i], l[i+1:]...)
				delete(q.bucketFor, n.key)
				return true
			}
		}
	}
	return false
}

func (q *KeyedCalendarQueue) removeByKey(key string) (*entry, bool) {
	if b, ok := q.bucketFor[key]; ok {
		if !(len(q.buckets) > b) {
			return nil, false
		}

		l := q.buckets[b]
		last := len(l)
		for i, e := range l {
			if e.key == key {
				if i == last {
					q.buckets[b] = l[:i]
					delete(q.bucketFor, key)
					return e, true
				} else {
					q.buckets[b] = append(l[:i], l[i+1:]...)
					delete(q.bucketFor, key)
					return e, true
				}
			}
		}
	}
	return nil, false
}

func (q *KeyedCalendarQueue) peek(b int) *entry {
	if !(len(q.buckets) > b) {
		return nil
	}

	l := q.buckets[b]
	if len(l) > 0 {
		return l[0]
	}
	return nil
}

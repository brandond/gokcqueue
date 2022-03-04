package gokcqueue

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

const initialBucketSize = 16

type entry struct {
	key  string
	when time.Time
	data interface{}
}

type entryList []*entry

type KeyedCalendarQueue struct {
	interrupt chan *entry
	bucketFor map[string]int
	buckets   []entryList
	events    chan interface{}
	next      *entry
}

func (q *KeyedCalendarQueue) insert(b int, n *entry) {
	l := q.buckets[b]
	pos := len(l)

	for i, e := range l {
		if e.when.Before(n.when) {
			pos = i + 1
			break
		}
	}

	if pos == len(l) {
		q.buckets[b] = append(l, n)
		return
	}

	l = append(l[:pos+1], l[pos:]...)
	l[pos] = n
	q.buckets[b] = l
}

func (q *KeyedCalendarQueue) remove(b int, key string) *entry {
	l := q.buckets[b]
	last := len(l)
	for i, e := range l {
		if e.key == key {
			if i == last {
				q.buckets[b] = l[:i]
				return e
			} else {
				q.buckets[b] = append(l[:i], l[i+1:]...)
				return e
			}
		}
	}
	return nil
}

func (q *KeyedCalendarQueue) pop(b int) *entry {
	l := q.buckets[b]
	if len(l) < 1 {
		return nil
	}
	q.buckets[b] = l[1:]
	return l[0]
}

func (q *KeyedCalendarQueue) peek(b int) *entry {
	l := q.buckets[b]
	if len(l) > 0 {
		return l[0]
	}
	return nil
}

func (q *KeyedCalendarQueue) Add(key string, when time.Time, data interface{}) (interface{}, bool) {
	if q.buckets == nil {
		q.buckets = make([]entryList, initialBucketSize)
	}

	if q.bucketFor == nil {
		q.bucketFor = map[string]int{}
	}

	// if an entry with this key already exists, remove the current one
	existing, ok := q.remove(key)
	if !ok {
		existing = &entry{}
	}

	created := &entry{
		key:  key,
		when: when,
		data: data,
	}

	if q.next == nil {
		// had nothing to wait on, interrupt to wait on the new entry
		defer q.sendInterrupt(created)
	} else {
		if created.when.Before(q.next.when) {
			// new entry will fire before the one we were waiting on
			defer q.sendInterrupt(created)
		} else if q.next == existing {
			// new entry replaced the entry that we were waiting on
			defer q.sendInterrupt(existing)
		}
	}

	q.add(created)
	return existing.data, ok
}

func (q *KeyedCalendarQueue) add(e *entry) {
	b := q.findBucket(e.when)
	l := insert(q.buckets[b], e)
	q.bucketFor[e.key] = b
	q.buckets[b] = l
}

func (q *KeyedCalendarQueue) Remove(key string) (time.Time, interface{}, error) {
	if e, ok := q.remove(key); ok {
		if q.next == e {
			defer q.sendInterrupt(e)
		}
		return e.when, e.data, nil
	}

	return time.Time{}, nil, errors.New("key not found")
}

func (q *KeyedCalendarQueue) remove(key string) (*entry, bool) {
	if b, ok := q.bucketFor[key]; ok {
		e, l := remove(q.buckets[b], key)
		q.buckets[b] = l
		delete(q.bucketFor, key)
		return e, true
	}
	return nil, false
}

func (q *KeyedCalendarQueue) Events() <-chan interface{} {
	if q.events == nil {
		q.events = make(chan interface{}, 1)
		q.interrupt = make(chan *entry, 1)
		go q.eventLoop(context.Background())
	}
	return q.events
}

func (q *KeyedCalendarQueue) findBucket(t time.Time) int {
	l := int64(len(q.buckets))
	return int((t.Unix() / l) % l)
}

func (q *KeyedCalendarQueue) sameEpoch(a time.Time, b time.Time) bool {
	l := int64(len(q.buckets))
	return int(a.Unix()/l) == int(b.Unix()/l)
}

func (q *KeyedCalendarQueue) sendInterrupt(e *entry) {
	if q.interrupt != nil {
		q.interrupt <- e
	}
}

func (q *KeyedCalendarQueue) eventLoop(ctx context.Context) {
	p := time.Time{}
	t := time.NewTimer(0)
	<-t.C

	for {
		if q.next == nil {
			q.next = q.getNextEntry(p)
		}

		// if we have an entry to wait for, reset the timer;
		// otherwise, leave it read so that we wait on an interrupt.
		if q.next != nil {
			if !t.Stop() {
				<-t.C
			}
			t.Reset(time.Until(q.next.when))
		}

		select {
		case <-t.C:
			q.events <- q.next.data
			p = q.next.when
			q.next = nil
		case e := <-q.interrupt:
			if q.next == e {
				// interrupted by removal of the entry we were waiting on - treat it as handled and move on
				p = q.next.when
				q.next = nil
			} else {
				// interrupted by addition of a new entry that will fire before the one we were waiting on
				if q.next != nil {
					// If we were waiting on an entry, re-insert it into the queue
					q.add(q.next)
				}
				// start waiting on the new one instead
				q.next = e
			}
		case <-ctx.Done():
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

	// Step through the next round of epochs looking for events in their buckets, also noting the earliest entry we find
	var n *entry
	l := len(q.buckets)
	for i := 1; i < l; i++ {
		t = t.Add(time.Millisecond * time.Duration(l))
		b = q.findBucket(t)
		e := q.peek(b)
		if e != nil {
			if q.sameEpoch(t, e.when) {
				return q.pop(b)
			}
			if n == nil || e.when.Before(n.when) {
				n = e
			}
		}
	}

	// If we made it through all the buckets without finding an event in any of the epochs, jump forward to the next earliest event.
	if n != nil {
		b := q.findBucket(n.when)
		return q.pop(b)
	}
	// If we didn't find an event at all then all the buckets are empty, which is fine.
	return nil
}

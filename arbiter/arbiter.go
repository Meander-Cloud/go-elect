package arbiter

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	"github.com/Meander-Cloud/go-elect/config"
)

type Arbiter struct {
	c       *config.Config
	s       *scheduler.Scheduler[Group]
	eventpl sync.Pool
	eventch chan *event
}

func NewArbiter(c *config.Config) *Arbiter {
	var eventChannelLength uint16
	if c.EventChannelLength == 0 {
		eventChannelLength = config.EventChannelLength
	} else {
		eventChannelLength = c.EventChannelLength
	}

	a := &Arbiter{
		c: c,
		s: scheduler.NewScheduler[Group](
			&scheduler.Options{
				EventChannelLength: eventChannelLength,
				LogPrefix:          "Arbiter",
				LogDebug:           c.LogDebug,
			},
		),
		eventpl: sync.Pool{
			New: func() any {
				return newEvent()
			},
		},
		eventch: make(chan *event, eventChannelLength),
	}

	// add eventch
	a.s.ProcessAsync(
		&scheduler.ScheduleAsyncEvent[Group]{
			AsyncVariant: scheduler.NewAsyncVariant(
				false,
				nil,
				a.eventch,
				func(_ *scheduler.Scheduler[Group], _ *scheduler.AsyncVariant[Group], recv interface{}) {
					a.handle(recv)
				},
				func(_ *scheduler.Scheduler[Group], v *scheduler.AsyncVariant[Group]) {
					log.Printf("%s: eventch released, select count: %d", c.LogPrefix, v.SelectCount)
				},
			),
		},
	)

	// ownership of internal state is transferred to scheduler goroutine
	a.s.RunAsync()

	return a
}

func (a *Arbiter) Shutdown() {
	a.s.Shutdown() // wait
}

func (a *Arbiter) Scheduler() *scheduler.Scheduler[Group] {
	return a.s
}

func (a *Arbiter) getEvent() *event {
	evtAny := a.eventpl.Get()
	evt, ok := evtAny.(*event)
	if !ok {
		err := fmt.Errorf("%s: failed to cast event, evtAny=%#v", a.c.LogPrefix, evtAny)
		log.Printf("%s", err.Error())
		panic(err)
	}
	return evt
}

func (a *Arbiter) returnEvent(evt *event) {
	// recycle event
	evt.reset()
	a.eventpl.Put(evt)
}

// scheduler goroutine
func (a *Arbiter) handle(recv interface{}) {
	evt, ok := recv.(*event)
	if !ok {
		log.Printf("%s: failed to cast event, recv=%#v", a.c.LogPrefix, recv)
		return
	}
	defer a.returnEvent(evt)

	t1 := time.Now().UTC()

	func() {
		defer func() {
			rec := recover()
			if rec != nil {
				log.Printf(
					"%s: functor recovered from panic: %+v",
					a.c.LogPrefix,
					rec,
				)
			}
		}()
		evt.f()
	}()

	t2 := time.Now().UTC()

	// log event lifecycle
	log.Printf(
		"%s: event goQueueWait=%dus, evtFuncElapsed=%dus",
		a.c.LogPrefix,
		t1.Sub(evt.t0).Microseconds(),
		t2.Sub(t1).Microseconds(),
	)
}

// any goroutine
func (a *Arbiter) Dispatch(f func()) error {
	evt := a.getEvent()
	evt.f = f
	evt.t0 = time.Now().UTC()

	select {
	case a.eventch <- evt:
	default:
		err := fmt.Errorf("%s: failed to push to eventch", a.c.LogPrefix)
		log.Printf("%s", err.Error())

		a.returnEvent(evt)
		return err
	}

	return nil
}

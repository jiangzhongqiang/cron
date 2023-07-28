package cron

import "time"

const INFINITY_LOOP = -1

// LimitedDelaySchedule Delayed Schedule with a limit on the number of excutions
type LimitedDelaySchedule struct {
	Delay time.Duration
	// Loop exec times, default -1 that represent unlimited
	Loop int64
}

func (d *LimitedDelaySchedule) Next(t time.Time) time.Time {
	if d.Loop > 0 {
		d.Loop--
	}
	return t.Add(d.Delay)
}

func (d *LimitedDelaySchedule) HasNext() bool {
	return d.Loop == -1 || d.Loop > 0
}

func NewLimitedDelaySchedule(delay time.Duration, loop int64) (Schedule, error) {
	if loop <= 0 {
		loop = INFINITY_LOOP
	}
	return &LimitedDelaySchedule{
		Delay: delay,
		Loop:  loop,
	}, nil
}

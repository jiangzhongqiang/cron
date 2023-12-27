package cron

import "time"

// FixedTimeSchedule execute at the fixed time
type FixedTimeSchedule struct {
	ExecTime time.Time
	// Loop exec times, fixed 1
	Loop int64
}

func (d *FixedTimeSchedule) Next(_ time.Time) time.Time {
	if d.Loop > 0 {
		d.Loop--
	}
	return d.ExecTime
}

func (d *FixedTimeSchedule) HasNext() bool {
	return d.Loop == INFINITY_LOOP || d.Loop > 0
}

func NewFixedTimeSchedule(execTime time.Time) (Schedule, error) {
	return &FixedTimeSchedule{
		ExecTime: execTime,
		Loop:     1,
	}, nil
}

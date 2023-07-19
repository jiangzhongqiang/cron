package cron

import "time"

// Clock is the interface used for custom time
type Clock interface {

	// Now returns the current time.
	Now() time.Time

	// Location retruns the already set Location
	Location() *time.Location
}

// DefaultClock is the default clock used by Cron in operations that require time.
var DefaultClock = LocalClock()

type customClock struct {
	loc *time.Location
}

func (c *customClock) Now() time.Time {
	return time.Now().In(c.loc)
}

func (c *customClock) Location() *time.Location {
	return c.loc
}

func LocalClock() Clock {
	return &customClock{
		loc: time.Local,
	}
}

func UTCClock() Clock {
	return &customClock{
		loc: time.UTC,
	}
}

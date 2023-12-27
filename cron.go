package cron

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   EntryHeap
	chain     Chain
	stop      chan struct{}
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry
	running   bool
	logger    Logger
	clock     Clock
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
	HasNext() bool
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//   Time Zone
//     Description: The time zone in which schedules are interpreted
//     Default:     time.Local
//
//   Parser
//     Description: Parser converts cron spec strings into cron.Schedules.
//     Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//   Chain
//     Description: Wrap submitted jobs to customize behavior.
//     Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		clock:     DefaultClock,
		parser:    standardParser,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.location = c.clock.Location()
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFuncAtFixedTime adds a func to the Cron to be executed at the time of given
func (c *Cron) AddFuncAtFixedTime(execTime time.Time, cmd func()) (EntryID, error) {
	return c.AddJobAtFixedTime(execTime, FuncJob(cmd))
}

// AddJobAtFixedTime adds a Job to the Cron to be executed at the fixed time of given
func (c *Cron) AddJobAtFixedTime(execTime time.Time, cmd Job) (EntryID, error) {
	schedule, err := NewFixedTimeSchedule(execTime)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// AddFuncWithDelay adds a func to the Cron to be executed with a delay time of given, only once
func (c *Cron) AddFuncWithDelay(delay time.Duration, cmd func()) (EntryID, error) {
	return c.AddJobWithLoopDelay(delay, 1, FuncJob(cmd))
}

// AddFuncWithLoopDelay adds a func to the Cron to be executed with a delay time of given each time, repeat the given times
// @param loop: the given times
func (c *Cron) AddFuncWithLoopDelay(delay time.Duration, loop int64, cmd func()) (EntryID, error) {
	return c.AddJobWithLoopDelay(delay, loop, FuncJob(cmd))
}

// AddJobWithDelay adds a Job to the Cron to be executed after a delay time of given, only once
func (c *Cron) AddJobWithDelay(delay time.Duration, cmd Job) (EntryID, error) {
	return c.AddJobWithLoopDelay(delay, 1, cmd)
}

// AddJobWithLoopDelay adds a Job to the Cron to be executed after a delay time of given each time and repeat the given times
// @param loop: the given times
func (c *Cron) AddJobWithLoopDelay(delay time.Duration, loop int64, cmd Job) (EntryID, error) {
	schedule, err := NewLimitedDelaySchedule(delay, loop)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if !c.running {
		heap.Push(&c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	sortedEntries := new(EntryHeap)
	for len(c.entries) > 0 {
		entry := heap.Pop(&c.entries).(*Entry)
		if entry.Schedule.HasNext() {
			entry.Next = entry.Schedule.Next(now)
			heap.Push(sortedEntries, entry)
			c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
		}
	}
	c.entries = *sortedEntries

	var timer *time.Timer
	for {
		// Determine the next entry to run.
		// User min-heap no need sort anymore
		//sort.Sort(byTime(c.entries))

		var delay time.Duration
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			delay = 1000000 * time.Hour
		} else {
			delay = c.entries[0].Next.Sub(now)
		}

		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			c.resetTimer(timer, delay)
		}

		for {
			select {
			case <-timer.C:
				now = c.now()
				c.logger.Info("wake", "now", now)
				// Run every entry whose next time was less than now
				for {
					e := c.entries.Peek()
					if e == nil || e.Next.After(now) || e.Next.IsZero() {
						break
					}
					e = heap.Pop(&c.entries).(*Entry)
					c.startJob(e.WrappedJob)
					e.Prev = e.Next
					if e.Schedule.HasNext() {
						e.Next = e.Schedule.Next(now)
						heap.Push(&c.entries, e)
						c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
					}
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				if newEntry.Schedule.HasNext() {
					newEntry.Next = newEntry.Schedule.Next(now)
					heap.Push(&c.entries, newEntry)
					c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)
				}
			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				c.logger.Info("stop")
				return

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)
			}

			break
		}
	}
}

// resetTimer reset timer
func (c *Cron) resetTimer(timer *time.Timer, delay time.Duration) {
	// timer.Stop It returns true if the call stops the timer, false if the timer has already expired or been stopped.
	// timer.Stop It does not close the channel
	if !timer.Stop() {
		select {
		case <-timer.C: // Try to drain the channel to ensure the channel is empty after a call to Stop.
		default:
		}
	}
	// timer.Reset should always be invoked on stopped or expired channels
	timer.Reset(delay)
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run()
	}()
}

// now returns current time
func (c *Cron) now() time.Time {
	return c.clock.Now()
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{} //不再执行将来的定时任务
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c.jobWaiter.Wait() //等待所有正在执行的任务完成后，再执行cancel
		cancel()
	}()
	return ctx
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	for idx, e := range c.entries {
		if e.ID == id {
			heap.Remove(&c.entries, idx)
			return
		}
	}
}

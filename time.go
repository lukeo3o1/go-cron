package cron

import (
	"fmt"
	"strings"
	"time"
)

type Time interface {
	Next(time.Time) time.Time
	String() string
}

type Duration struct {
	d time.Duration
}

func (d *Duration) Next(at time.Time) time.Time {
	return at.Add(d.d)
}

func (d *Duration) String() string {
	return fmt.Sprintf("Every: %s", d.d)
}

func Every(d time.Duration) *Duration {
	return &Duration{
		d: d,
	}
}

type ClockTime struct {
	hour int
	min  int
	sec  int
}

func Clock(hour, min, sec int) *ClockTime {
	t := time.Date(1, time.January, 1, hour, min, sec, 0, time.UTC)
	return &ClockTime{
		hour: t.Hour(),
		min:  t.Minute(),
		sec:  t.Second(),
	}
}

func (t *ClockTime) Next(at time.Time) time.Time {
	next := time.Date(at.Year(), at.Month(), at.Day(), t.hour, t.min, t.sec, 0, at.Location())
	if next.After(at) {
		return next
	}
	return next.AddDate(0, 0, 1)
}

func (t *ClockTime) String() string {
	return fmt.Sprintf("Clock: %02d:%02d:%02d", t.hour, t.min, t.sec)
}

type WeekTime struct {
	clock    *ClockTime
	weekdays [7]bool
}

func Week(clock *ClockTime, weeks ...time.Weekday) *WeekTime {
	t := WeekTime{
		clock: clock,
	}
	if len(weeks) == 0 {
		for i := range t.weekdays {
			t.weekdays[i] = true
		}
		return &t
	}
	for _, w := range weeks {
		t.weekdays[w] = true
	}
	return &t
}

func (t *WeekTime) Next(at time.Time) time.Time {
	next := t.clock.Next(at)
	for {
		if t.weekdays[next.Weekday()] {
			if next.After(at) {
				return next
			}
		}
		next = next.AddDate(0, 0, 1)
	}
}

func (t *WeekTime) String() string {
	var b strings.Builder
	b.WriteString(t.clock.String())
	b.WriteString(", Week: ")
	count := 0
	for i := range t.weekdays {
		if t.weekdays[i] {
			if count > 0 {
				b.WriteString(" | ")
			}
			b.WriteString(time.Weekday(i).String())
			count++
		}
	}
	return b.String()
}

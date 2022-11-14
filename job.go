package cron

import "context"

type Job interface {
	Run(ctx context.Context)
}

type job[ID comparable] struct {
	context.Context
	cancel context.CancelFunc
	id     ID
	time   Time
	job    Job
}

type JobFunc func(ctx context.Context)

func (f JobFunc) Run(ctx context.Context) { f(ctx) }

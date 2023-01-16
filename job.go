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

func (j *job[JobID]) SetContext(ctx context.Context) {
	j.Context = ctx
}

func (j *job[JobID]) SetCancelContext(ctx context.Context) {
	if ctx != nil {
		j.Context, j.cancel = context.WithCancel(ctx)
	}
}

type JobFunc func(ctx context.Context)

func (f JobFunc) Run(ctx context.Context) { f(ctx) }

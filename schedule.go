package cron

import (
	"context"
	"sync"
	"time"
)

type Scheduler[JobID comparable] struct {
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	jobmap       map[JobID]*job[JobID]
	joblist      []*job[JobID]
	add          chan *job[JobID]
	remove       chan JobID
	runjob       chan JobID
	stop         chan struct{}
	expiredTimer chan *Timer[JobID]
}

func NewScheduler[JobID comparable]() *Scheduler[JobID] {
	return &Scheduler[JobID]{
		jobmap:       map[JobID]*job[JobID]{},
		add:          make(chan *job[JobID]),
		remove:       make(chan JobID),
		runjob:       make(chan JobID),
		stop:         make(chan struct{}, 1),
		expiredTimer: make(chan *Timer[JobID]),
	}
}

type Timer[JobID comparable] struct {
	mu    sync.Mutex
	time  Time
	timer *time.Timer
	jobs  []*job[JobID]
}

func (t *Timer[JobID]) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timer.Stop()
}

func (t *Timer[JobID]) addJob(j *job[JobID]) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.jobs = append(t.jobs, j)
}

func (t *Timer[JobID]) rangeJob(cb func(j *job[JobID]) bool) {
	deleted := 0
	for idx := range t.jobs {
		i := idx - deleted
		j := t.jobs[i]
		select {
		case <-j.Done():
			t.jobs[i] = nil
			t.jobs = append(t.jobs[:i], t.jobs[i+1:]...)
			deleted++
		default:
			if !cb(j) {
				return
			}
		}
	}
}

func rangeAllDoNothing[JobID comparable](j *job[JobID]) bool { return true }

func runAllJob[JobID comparable](j *job[JobID]) bool {
	go j.job.Run(j.Context)
	return true
}

func (s *Scheduler[JobID]) newTimer(st Time) *Timer[JobID] {
	now := time.Now()
	if d, ok := st.(Deadline); ok && d.IsExpired(now) {
		return nil
	}

	t := &Timer[JobID]{}
	ctx := s.ctx
	t.time = st
	t.timer = time.AfterFunc(time.Until(t.time.Next(time.Now())), func() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		now := time.Now()

		if d, ok := t.time.(Deadline); ok && d.IsExpired(now) {
			select {
			case <-ctx.Done():
			case s.expiredTimer <- t:
			}
			return
		}

		d := time.Until(t.time.Next(now))
		t.mu.Lock()
		defer t.mu.Unlock()
		t.timer.Reset(d)
		t.rangeJob(runAllJob[JobID])
	})
	return t
}

func (s *Scheduler[JobID]) loadOrStoreTimer(timers map[string]*Timer[JobID], st Time) *Timer[JobID] {
	if timers == nil {
		return nil
	}
	key := st.String()
	t := timers[key]
	if t == nil {
		if t = s.newTimer(st); t != nil {
			timers[key] = t
		}
	}
	return t
}

func isRemoveCtxDoneJobAndStopWhenJobListEmptyTimer[JobID comparable](t *Timer[JobID]) bool {
	t.rangeJob(rangeAllDoNothing[JobID])
	return len(t.jobs) == 0
}

func alwaysRemoveTimer[JobID comparable](t *Timer[JobID]) bool { return true }

func removeTimer[JobID comparable](timers map[string]*Timer[JobID], time Time, isRemove func(t *Timer[JobID]) bool) {
	ts := time.String()
	t := timers[ts]
	t.mu.Lock()
	defer t.mu.Unlock()
	if !isRemove(t) {
		return
	}
	t.timer.Stop()
	timers[ts] = nil
	delete(timers, ts)
}

func (s *Scheduler[JobID]) addJob2Timer(timers map[string]*Timer[JobID], j *job[JobID]) {
	if timers != nil {
		if t := s.loadOrStoreTimer(timers, j.time); t != nil {
			t.addJob(j)
		}
	}
}

func (s *Scheduler[JobID]) addJob(timers map[string]*Timer[JobID], j *job[JobID]) {
	j.SetContext(s.ctx)
	s.joblist = append(s.joblist, j)
	s.addJob2Timer(timers, j)
}

func (s *Scheduler[JobID]) setJob(timers map[string]*Timer[JobID], j *job[JobID]) {
	oldjob := s.jobmap[j.id]
	if oldjob != nil && oldjob.cancel != nil {
		oldjob.cancel()
	}
	j.SetCancelContext(s.ctx)
	s.jobmap[j.id] = j
	s.addJob2Timer(timers, j)
}

func (s *Scheduler[JobID]) removeJob(id JobID) *job[JobID] {
	j := s.jobmap[id]
	if j == nil {
		return nil
	}
	if j.cancel != nil {
		j.cancel()
	}
	delete(s.jobmap, id)
	return j
}

func (s *Scheduler[JobID]) runJob(id JobID) {
	j := s.jobmap[id]
	if j == nil {
		return
	}
	go j.job.Run(j.Context)
}

func (s *Scheduler[JobID]) run() {
	var null JobID
	s.mu.Lock()
	defer s.mu.Unlock()
	timers := map[string]*Timer[JobID]{}
	for _, j := range s.jobmap {
		j.SetCancelContext(s.ctx)
		s.addJob2Timer(timers, j)
	}
	for _, j := range s.joblist {
		j.SetContext(s.ctx)
		s.addJob2Timer(timers, j)
	}
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case j := <-s.add:
			if j.id != null {
				s.setJob(timers, j)
				break
			}
			s.addJob(timers, j)
		case id := <-s.remove:
			j := s.removeJob(id)
			removeTimer(timers, j.time, isRemoveCtxDoneJobAndStopWhenJobListEmptyTimer[JobID])
		case id := <-s.runjob:
			s.runJob(id)
		case <-s.ctx.Done():
			for _, t := range timers {
				t.Stop()
			}
			return
		case <-s.stop:
			s.cancel()
		case t := <-s.expiredTimer:
			removeTimer(timers, t.time, alwaysRemoveTimer[JobID])
		case <-ticker.C:
			for _, t := range timers {
				removeTimer(timers, t.time, isRemoveCtxDoneJobAndStopWhenJobListEmptyTimer[JobID])
			}
		}
	}
}

func (s *Scheduler[JobID]) Set(id JobID, t Time, j Job) {
	job := job[JobID]{
		id:   id,
		time: t,
		job:  j,
	}
	if !s.mu.TryLock() {
		select {
		case s.add <- &job:
		default:
			s.Set(id, t, j)
		}
		return
	}
	defer s.mu.Unlock()
	s.setJob(nil, &job)
}

func (s *Scheduler[JobID]) Add(t Time, j Job) {
	job := job[JobID]{
		time: t,
		job:  j,
	}
	if !s.mu.TryLock() {
		select {
		case s.add <- &job:
		default:
			s.Add(t, j)
		}
		return
	}
	defer s.mu.Unlock()
	s.addJob(nil, &job)
}

func (s *Scheduler[JobID]) Remove(id JobID) {
	if !s.mu.TryLock() {
		select {
		case s.remove <- id:
		default:
			s.Remove(id)
		}
		return
	}
	defer s.mu.Unlock()
	s.removeJob(id)
}

func (s *Scheduler[JobID]) Run(id JobID) {
	if !s.mu.TryLock() {
		select {
		case s.runjob <- id:
		default:
			s.Run(id)
		}
		return
	}
	defer s.mu.Unlock()
	s.runJob(id)
}

func (s *Scheduler[JobID]) Start(ctx context.Context) {
	if !s.mu.TryLock() {
		return
	}
	defer s.mu.Unlock()
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
		default:
			return
		}
	}
	select {
	case <-s.stop:
	default:
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	go s.run()
}

func (s *Scheduler[JobID]) Stop() {
	if s.mu.TryLock() {
		s.mu.Unlock()
		return
	}
	select {
	case s.stop <- struct{}{}:
	default:
	}
	s.Wait()
}

func (s *Scheduler[JobID]) Wait() {
	s.mu.Lock()
	ctx := s.ctx
	s.mu.Unlock()
	if ctx == nil {
		return
	}
	<-ctx.Done()
}

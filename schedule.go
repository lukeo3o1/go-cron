package cron

import (
	"context"
	"sync"
	"time"
)

type Scheduler[JobID comparable] struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	jobmap  map[JobID]*job[JobID]
	joblist []*job[JobID]
	add     chan *job[JobID]
	remove  chan JobID
	runjob  chan JobID
	stop    chan struct{}
}

func NewScheduler[JobID comparable]() *Scheduler[JobID] {
	return &Scheduler[JobID]{
		jobmap: map[JobID]*job[JobID]{},
		add:    make(chan *job[JobID]),
		remove: make(chan JobID),
		runjob: make(chan JobID),
		stop:   make(chan struct{}, 1),
	}
}

type Timer[JobID comparable] struct {
	rw    sync.RWMutex
	time  Time
	timer *time.Timer
	jobs  []*job[JobID]
}

func (t *Timer[JobID]) addJob(j *job[JobID]) {
	t.rw.Lock()
	defer t.rw.Unlock()
	t.jobs = append(t.jobs, j)
}

func (s *Scheduler[JobID]) newTimer(st Time) *Timer[JobID] {
	t := &Timer[JobID]{}
	ctx := s.ctx
	t.time = st
	t.timer = time.AfterFunc(time.Until(t.time.Next(time.Now())), func() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		t.timer.Reset(time.Until(t.time.Next(time.Now())))

		t.rw.RLock()
		defer t.rw.RUnlock()

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
				go j.job.Run(j.Context)
			}
		}
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
		t = s.newTimer(st)
		timers[key] = t
	}
	return t
}

func (s *Scheduler[JobID]) addJob(timers map[string]*Timer[JobID], j *job[JobID]) {
	if timers != nil {
		t := s.loadOrStoreTimer(timers, j.time)
		defer t.addJob(j)
	}
	if s.ctx != nil {
		j.Context = s.ctx
	}
	s.joblist = append(s.joblist, j)
}

func (s *Scheduler[JobID]) setJob(timers map[string]*Timer[JobID], j *job[JobID]) {
	if timers != nil {
		t := s.loadOrStoreTimer(timers, j.time)
		defer t.addJob(j)
	}
	oldjob := s.jobmap[j.id]
	if oldjob != nil && oldjob.cancel != nil {
		oldjob.cancel()
	}
	if s.ctx != nil {
		j.Context, j.cancel = context.WithCancel(s.ctx)
	}
	s.jobmap[j.id] = j
}

func (s *Scheduler[JobID]) removeJob(id JobID) {
	j := s.jobmap[id]
	if j == nil {
		return
	}
	if j.cancel != nil {
		j.cancel()
	}
	delete(s.jobmap, id)
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
		s.setJob(timers, j)
	}
	for _, j := range s.joblist {
		s.addJob(timers, j)
	}
	for {
		select {
		case j := <-s.add:
			if j.id != null {
				s.setJob(timers, j)
				break
			}
			s.addJob(timers, j)
		case id := <-s.remove:
			s.removeJob(id)
		case id := <-s.runjob:
			s.runJob(id)
		case <-s.ctx.Done():
			return
		case <-s.stop:
			s.cancel()
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

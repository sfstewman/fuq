package proto

import "sync"

type Sequencer interface {
	Next() uint32
	Current() uint32
	Update(n uint32)
}

type LockedSequence struct {
	mu  sync.Mutex
	seq uint32
}

func (s *LockedSequence) Lock() {
	s.mu.Lock()
}

func (s *LockedSequence) Unlock() {
	s.mu.Unlock()
}

func (s *LockedSequence) Next() uint32 {
	s.Lock()
	defer s.Unlock()
	seq := s.seq
	s.seq++
	return seq
}

func (s *LockedSequence) Current() uint32 {
	s.Lock()
	defer s.Unlock()
	return s.seq
}

func (s *LockedSequence) Update(n uint32) {
	s.Lock()
	defer s.Unlock()
	if n > s.seq {
		s.seq = n
	}
}

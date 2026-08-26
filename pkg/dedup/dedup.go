package dedup

import "sync"

type Store struct { seen sync.Map }

func (s *Store) Accept(id string) bool {
	if id == "" { return false }
	_, loaded := s.seen.LoadOrStore(id, struct{}{})
	return !loaded
}

func (s *Store) Len() int {
	n := 0
	s.seen.Range(func(_, _ any) bool { n++; return true })
	return n
}

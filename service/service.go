package service

import (
	"sync"
)

type Runner interface {
	Run(done chan struct{}, wg *sync.WaitGroup) error
}

type Service struct {
	wg        sync.WaitGroup
	done      chan struct{}
	isRunning bool
	runner    Runner
}

func New(runner Runner) *Service {
	return &Service{runner: runner}
}

func (s *Service) canStart() bool {
	if s.isRunning {
		return false
	}
	s.isRunning = true
	return true
}

func (s *Service) Start() error {
	if !s.canStart() {
		return nil
	}
	s.done = make(chan struct{})
	return s.runner.Run(s.done, &s.wg)
}

func (s *Service) Stop() {
	if s.done == nil {
		return
	}
	close(s.done)
	s.done = nil
	s.wg.Wait()
	s.isRunning = false
}

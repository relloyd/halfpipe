package stats

type MockStatsManager struct{}

func (s *MockStatsManager) StartDumping() {}

func (s *MockStatsManager) StopDumping() {}

func (s *MockStatsManager) AddStepWatcher(stepName string) *StepWatcher {
	return nil
}

func NewMockStatsManager() *MockStatsManager {
	return &MockStatsManager{}
}

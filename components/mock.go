package components

type MockComponentWaiter struct {
	count int
}

func (cw *MockComponentWaiter) Add() {
	cw.count++
}

func (cw *MockComponentWaiter) Done() {
	cw.count--
}

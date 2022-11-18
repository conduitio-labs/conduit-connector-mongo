// Code generated by MockGen. DO NOT EDIT.
// Source: source/source.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	conduit_connector_sdk "github.com/conduitio/conduit-connector-sdk"
	gomock "github.com/golang/mock/gomock"
)

// MockIterator is a mock of Iterator interface.
type MockIterator struct {
	ctrl     *gomock.Controller
	recorder *MockIteratorMockRecorder
}

// MockIteratorMockRecorder is the mock recorder for MockIterator.
type MockIteratorMockRecorder struct {
	mock *MockIterator
}

// NewMockIterator creates a new mock instance.
func NewMockIterator(ctrl *gomock.Controller) *MockIterator {
	mock := &MockIterator{ctrl: ctrl}
	mock.recorder = &MockIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIterator) EXPECT() *MockIteratorMockRecorder {
	return m.recorder
}

// HasNext mocks base method.
func (m *MockIterator) HasNext(arg0 context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasNext", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HasNext indicates an expected call of HasNext.
func (mr *MockIteratorMockRecorder) HasNext(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasNext", reflect.TypeOf((*MockIterator)(nil).HasNext), arg0)
}

// Next mocks base method.
func (m *MockIterator) Next(arg0 context.Context) (conduit_connector_sdk.Record, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next", arg0)
	ret0, _ := ret[0].(conduit_connector_sdk.Record)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Next indicates an expected call of Next.
func (mr *MockIteratorMockRecorder) Next(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockIterator)(nil).Next), arg0)
}

// Stop mocks base method.
func (m *MockIterator) Stop(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockIteratorMockRecorder) Stop(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockIterator)(nil).Stop), arg0)
}

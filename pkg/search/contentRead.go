package search

import (
	"sync/atomic"
	"unsafe"
)

type LasyRead[T any] struct {
	readF  func(path string, tmpBuf []byte) ([]byte, T)
	path   string
	readed atomic.Bool
	arg    T
	buf    []byte
	bufS   string
}

func NewLasyRead[T any](ifNeedReadFunc func(path string, content []byte) ([]byte, T), path string, content []byte) *LasyRead[T] {
	return &LasyRead[T]{readF: ifNeedReadFunc, path: path, buf: content}
}

func NewNoLasyRead[T any](content []byte) (l *LasyRead[T]) {
	l = &LasyRead[T]{buf: content}
	l.readed.Store(true)
	return
}

func (t *LasyRead[T]) Read() (read bool) {
	if read = t.readed.CompareAndSwap(false, true); read {
		t.buf, t.arg = t.readF(t.path, t.buf)
		t.bufS = unsafe.String(unsafe.SliceData(t.Byte()), len(t.Byte()))
	}
	return
}

func (t *LasyRead[T]) Arg() T {
	return t.arg
}

func (t *LasyRead[T]) Byte() []byte {
	t.Read()
	return t.buf
}

func (t *LasyRead[T]) String() string {
	t.Read()
	return t.bufS
}

package search

import (
	"sync/atomic"
	"unsafe"
)

type LasyRead struct {
	readF  func(path string) []byte
	path   string
	readed atomic.Bool
	buf    []byte
}

func NewLasyRead(ifNeedReadFunc func(path string) []byte, path string) *LasyRead {
	return &LasyRead{readF: ifNeedReadFunc, path: path}
}

func NewNoLasyRead(content []byte) (l *LasyRead) {
	l = &LasyRead{buf: content}
	l.readed.Store(true)
	return
}

func (t *LasyRead) Read() (read bool) {
	if read = t.readed.CompareAndSwap(false, true); read {
		t.buf = t.readF(t.path)
	}
	return
}

func (t *LasyRead) Byte() []byte {
	t.Read()
	return t.buf
}

func (t *LasyRead) String() string {
	return unsafe.String(unsafe.SliceData(t.Byte()), len(t.Byte()))
}

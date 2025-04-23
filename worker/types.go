package worker

import (
	"github.com/andys/new_name/anonymizer"
)

// WriterPool is a temporary interface until we implement the writer
type WriterPool interface {
	Submit(row anonymizer.Row) error
}

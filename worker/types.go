package worker

import (
	"github.com/andys/new_name/db"
)

// Row represents a single row of data with its table schema
type Row struct {
	Schema *db.TableSchema
	Data   map[string]interface{}
}

// WriterPool is a temporary interface until we implement the writer
type WriterPool interface {
	Submit(row Row) error
}

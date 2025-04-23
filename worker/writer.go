package worker

import (
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/andys/new_name/anonymizer"
	"github.com/andys/new_name/db"
)

// WriterProgress tracks the progress of writing operations
type WriterProgress struct {
	CurrentTable  string
	ProcessedRows atomic.Int64
	StartTime     time.Time
}

// Writer manages writing data to destination database using a worker pool
type Writer struct {
	destDB   *db.Connection
	pool     pond.Pool
	progress *WriterProgress
}

// NewWriter creates a new writer worker pool
func NewWriter(destDB *db.Connection, maxWorkers int) *Writer {
	return &Writer{
		destDB: destDB,
		pool:   pond.NewPool(maxWorkers, pond.WithQueueSize(100000)),
		progress: &WriterProgress{
			StartTime: time.Now(),
		},
	}
}

// Submit submits a row for writing to the destination database
func (w *Writer) Submit(row anonymizer.Row) {
	w.progress.CurrentTable = row.Schema.Name

	w.pool.SubmitErr(func() error {
		err := w.upsertRow(row)
		if err == nil {
			w.progress.ProcessedRows.Add(1)
		}
		return err
	})
}

// upsertRow handles the upsert operation for a single row
// This is a stub that will be implemented later
func (w *Writer) upsertRow(row anonymizer.Row) error {
	// TODO: Implement actual upsert logic in db/transfer.go
	return nil
}

// GetProgress returns the current progress
func (w *Writer) GetProgress() WriterProgress {
	return *w.progress
}

// Stop stops the worker pool and waits for all tasks to complete
func (w *Writer) Stop() {
	w.pool.StopAndWait()
}

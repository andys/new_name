package worker

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/andys/new_names/anonymizer"
	"github.com/andys/new_names/config"
	"github.com/andys/new_names/db"
)

// WriterProgress tracks the progress of writing operations
type WriterProgress struct {
	CurrentTable  string
	ProcessedRows atomic.Int64
	DeletedRows   atomic.Int64
	ErrorCount    atomic.Int64
	StartTime     time.Time
}

// Writer manages writing data to destination database using a worker pool
type Writer struct {
	destDB   *db.Connection
	pool     pond.Pool
	progress *WriterProgress
	cfg      *config.Config
}

// NewWriter creates a new writer worker pool
func NewWriter(destDB *db.Connection, maxWorkers int, cfg *config.Config) *Writer {
	return &Writer{
		destDB: destDB,
		pool:   pond.NewPool(maxWorkers, pond.WithQueueSize(maxWorkers*2000)),
		progress: &WriterProgress{
			StartTime: time.Now(),
		},
		cfg: cfg,
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
func (w *Writer) upsertRow(row anonymizer.Row) error {
	err := w.destDB.UpsertRow(row.Schema, row.Data)
	if err != nil {
		w.progress.ErrorCount.Add(1)
		if w.cfg.Debug {
			fmt.Fprintf(os.Stderr, "Error writing to table %s: %v\n", row.Schema.Name, err)
		}
	}
	return err
}

// GetProgress returns the current progress
func (w *Writer) GetProgress() WriterProgress {
	return *w.progress
}

// DeleteBatch submits a job to delete rows in a range except for the provided IDs.
// ids must be a pre-sorted, non-empty slice of interface{} representing the IDs to keep.
func (w *Writer) DeleteBatch(table string, idCol string, ids []interface{}) {
	if len(ids) == 0 {
		return
	}
	w.pool.SubmitErr(func() error {
		n, err := w.destDB.DeleteBatchWithCount(table, idCol, ids)
		if err == nil {
			w.progress.DeletedRows.Add(int64(n))
		}
		return err
	})
}

// StopAndWait stops the worker pool and waits for all tasks to complete
func (w *Writer) StopAndWait() {
	w.pool.StopAndWait()
}

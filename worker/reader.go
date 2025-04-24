package worker

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/andys/new_name/anonymizer"
	"github.com/andys/new_name/config"
	"github.com/andys/new_name/db"
)

// Progress tracks the progress of table processing
type Progress struct {
	CurrentTable    string
	TotalTables     int64
	ProcessedTables atomic.Int64
	StartTime       time.Time
}

// Reader manages reading data from source database using a worker pool
type Reader struct {
	sourceDB *db.Connection
	pool     pond.Pool
	progress *Progress
	writer   *Writer
	cfg      *config.Config
}

// NewReader creates a new reader worker pool
func NewReader(sourceDB *db.Connection, writer *Writer, maxWorkers int, cfg *config.Config) *Reader {
	return &Reader{
		sourceDB: sourceDB,
		pool:     pond.NewPool(maxWorkers),
		progress: &Progress{
			StartTime: time.Now(),
		},
		writer: writer,
		cfg:    cfg,
	}
}

// ProcessTables processes all tables using the worker pool
func (r *Reader) ProcessTables(schemas []db.TableSchema) error {
	r.progress.TotalTables = int64(len(schemas))
	group := r.pool.NewGroup()

	for _, schema := range schemas {
		tableSchema := schema // Create local copy for closure

		group.SubmitErr(func() error {
			r.progress.CurrentTable = tableSchema.Name
			err := r.process(&tableSchema)
			r.progress.ProcessedTables.Add(1)
			return err
		})
	}

	return group.Wait()
}

// process handles reading and processing a single table
func (r *Reader) process(schema *db.TableSchema) error {
	// Build query to select all rows from table
	query := fmt.Sprintf("SELECT * FROM %s", schema.Name)

	rows, err := r.sourceDB.GetDB().Query(query)
	if err != nil {
		if r.cfg.Debug {
			fmt.Fprintf(os.Stderr, "Error reading from table %s: %v\n", schema.Name, err)
		}
		return fmt.Errorf("failed to query table %s: %w", schema.Name, err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns for table %s: %w", schema.Name, err)
	}

	// Prepare value holders
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Process each row
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row from table %s: %w", schema.Name, err)
		}

		// Create data map
		data := make(map[string]interface{})
		for i, col := range columns {
			data[col] = values[i]
		}

		// Create row struct
		row := anonymizer.Row{
			Schema: schema,
			Data:   data,
		}

		// Anonymize the row
		anonymizer.Anonymize(&row)

		// Submit to writer
		r.writer.Submit(row)
	}

	return rows.Err()
}

// GetProgress returns the current progress
func (r *Reader) GetProgress() Progress {
	return *r.progress
}

// Stop stops the worker pool and waits for all tasks to complete
func (r *Reader) Stop() {
	r.pool.StopAndWait()
}

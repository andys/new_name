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
			var err error
			if tableSchema.HasID {
				err = r.processWithId(&tableSchema)
			} else {
				err = r.processWithoutId(&tableSchema)
			}
			r.progress.ProcessedTables.Add(1)
			return err
		})
	}

	return group.Wait()
}

// process handles reading and processing a single table
func (r *Reader) processWithoutId(schema *db.TableSchema) error {
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
		anonymizer.Anonymize(&row, r.cfg)

		// Submit to writer
		r.writer.Submit(row)
	}

	return rows.Err()
}

// GetProgress returns the current progress
func compareID(a, b interface{}) int {
	// Assumes integer IDs; expand as needed for other types
	ai, aok := a.(int64)
	bi, bok := b.(int64)
	if aok && bok {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	}
	// fallback: compare as string
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

// processWithId handles reading and processing a table with an ID column, in batches
func (r *Reader) processWithId(schema *db.TableSchema) error {
	const batchSize = 100

	// Find the name of the ID column
	var idCol string
	for _, col := range schema.Columns {
		if col.IsID {
			idCol = col.Name
			break
		}
	}
	if idCol == "" {
		return fmt.Errorf("no ID column found for table %s", schema.Name)
	}

	var lastID interface{}
	firstBatch := true

	for {
		var query string
		var args []interface{}
		if firstBatch {
			query = fmt.Sprintf(
				"SELECT * FROM %s ORDER BY %s LIMIT %d",
				schema.Name, idCol, batchSize,
			)
			args = []interface{}{}
			firstBatch = false
		} else {
			query = fmt.Sprintf(
				"SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT %d",
				schema.Name, idCol, idCol, batchSize,
			)
			args = []interface{}{lastID}
		}

		rows, err := r.sourceDB.GetDB().Query(query, args...)
		if err != nil {
			if r.cfg.Debug {
				fmt.Fprintf(os.Stderr, "Error reading from table %s: %v\n", schema.Name, err)
			}
			return fmt.Errorf("failed to query table %s: %w", schema.Name, err)
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return fmt.Errorf("failed to get columns for table %s: %w", schema.Name, err)
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rowCount := 0
		var maxID interface{}
		ids := make([]interface{}, 0, batchSize)
		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan row from table %s: %w", schema.Name, err)
			}

			data := make(map[string]interface{})
			for i, col := range columns {
				data[col] = values[i]
			}

			row := anonymizer.Row{
				Schema: schema,
				Data:   data,
			}

			anonymizer.Anonymize(&row, r.cfg)
			r.writer.Submit(row)
			rowCount++

			// Update maxID
			idVal := data[idCol]
			ids = append(ids, idVal)
			if maxID == nil || compareID(idVal, maxID) > 0 {
				maxID = idVal
			}
		}
		rows.Close()

		if rowCount > 0 {
			r.writer.DeleteBatch(schema.Name, idCol, ids)
		}

		if rowCount == 0 {
			break // No more rows
		}
		lastID = maxID
		if rowCount < batchSize {
			break // Last batch
		}
	}

	return nil
}

func (r *Reader) GetProgress() Progress {
	return *r.progress
}

// Stop stops the worker pool and waits for all tasks to complete
func (r *Reader) Stop() {
	r.pool.StopAndWait()
}

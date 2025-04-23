package anonymizer

import (
	"github.com/andys/new_name/db"
)

// Row represents a single row of data with its table schema
type Row struct {
	Schema *db.TableSchema
	Data   map[string]interface{}
}

// Anonymize performs data anonymization on a row
// For now this is just a stub - will be implemented with actual anonymization later
func Anonymize(row *Row) {
	// Stub - will be implemented later with actual anonymization logic
}

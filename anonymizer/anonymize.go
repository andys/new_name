package anonymizer

import (
	"strings"

	"github.com/andys/new_name/config"
	"github.com/andys/new_name/db"
	"github.com/brianvoe/gofakeit/v7"
)

// Row represents a single row of data with its table schema
type Row struct {
	Schema *db.TableSchema
	Data   map[string]interface{}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

func toUint64(v any) uint64 {
	switch n := v.(type) {
	case uint:
		return uint64(n)
	case uint8:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return n
	}
	return 0
}

// Anonymize performs data anonymization on a row
func Anonymize(row *Row, cfg *config.Config) {
	table := row.Schema.Name
	fields, ok := cfg.AnonymizeFields[table]
	if !ok {
		return
	}
	fieldSet := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		fieldSet[f] = struct{}{}
	}

	for _, col := range row.Schema.Columns {
		if _, shouldAnon := fieldSet[col.Name]; !shouldAnon {
			continue
		}
		val := row.Data[col.Name]
		// Only anonymize non-nil values
		if val == nil {
			continue
		}
		// Skip if blank string
		if s, ok := val.(string); ok && strings.TrimSpace(s) == "" {
			continue
		}
		// Skip if numerically zero
		switch v := val.(type) {
		case int, int8, int16, int32, int64:
			if toInt64(v) == 0 {
				continue
			}
		case uint, uint8, uint16, uint32, uint64:
			if toUint64(v) == 0 {
				continue
			}
		case float32:
			if v == 0.0 {
				continue
			}
		case float64:
			if v == 0.0 {
				continue
			}
		}

		var fakeVal any
		lowerName := strings.ToLower(col.Name)
		maxLen := col.MaxLength
		if maxLen == 0 {
			maxLen = 255
		}
		colType := strings.ToLower(col.Type)
		switch {
		case strings.Contains(colType, "int"):
			fakeVal = gofakeit.Int64()
		case strings.Contains(colType, "float") || strings.Contains(colType, "double") || strings.Contains(colType, "real") || strings.Contains(colType, "numeric") || strings.Contains(colType, "decimal"):
			fakeVal = gofakeit.Float64()
		case strings.Contains(lowerName, "email"):
			fakeVal = gofakeit.Email()
		case strings.Contains(lowerName, "phone"):
			fakeVal = gofakeit.Phone()
		case strings.Contains(lowerName, "name"):
			fakeVal = gofakeit.Name()
		default:
			if maxLen >= 50 {
				fakeVal = gofakeit.Sentence(5)
			} else {
				fakeVal = gofakeit.LetterN(uint(maxLen))
			}
		}
		// Truncate if needed
		switch v := fakeVal.(type) {
		case string:
			if len(v) > maxLen {
				fakeVal = v[:maxLen]
			}
		}
		row.Data[col.Name] = fakeVal
	}
}

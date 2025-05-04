package db

import (
	"fmt"
)

// TableSchema represents the structure of a database table
type TableSchema struct {
	Name    string
	Columns []ColumnSchema
	HasID   bool // Indicates if table has an ID field for upsert logic
}

// ColumnSchema represents the structure of a table column
type ColumnSchema struct {
	Name      string
	Type      string
	IsID      bool // True if this is an ID column
	Nullable  bool
	MaxLength int // Maximum length for varchar fields
}

// GetSchema retrieves the database schema for all tables
func (c *Connection) GetSchema() ([]TableSchema, error) {
	switch c.Type {
	case MySQL:
		return c.getMySQLSchema()
	case PostgreSQL:
		return c.getPostgresSchema()
	default:
		return nil, fmt.Errorf("unsupported database type: %s", c.Type)
	}
}

func (c *Connection) getMySQLSchema() ([]TableSchema, error) {
	// Get current database name
	var dbName string
	err := c.db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to get database name: %w", err)
	}

	// Query to get tables and their columns
	query := `
        SELECT 
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as IS_NULLABLE,
            CASE WHEN c.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END as IS_PRIMARY,
            COALESCE(c.CHARACTER_MAXIMUM_LENGTH, 0) as MAX_LENGTH
        FROM information_schema.TABLES t
        JOIN information_schema.COLUMNS c 
            ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_SCHEMA = ?
            AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION`

	return c.processSchemaRows(query, dbName)
}

func (c *Connection) getPostgresSchema() ([]TableSchema, error) {
	// Query to get tables and their columns
	query := `
        SELECT 
            t.table_name,
            c.column_name,
            c.data_type,
            CASE WHEN c.is_nullable = 'YES' THEN 1 ELSE 0 END as is_nullable,
            CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END as is_primary,
            COALESCE(c.character_maximum_length, 0) as max_length
        FROM information_schema.tables t
        JOIN information_schema.columns c 
            ON t.table_name = c.table_name
        LEFT JOIN (
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        ) pk ON t.table_name = pk.table_name 
            AND c.column_name = pk.column_name
        WHERE t.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name, c.ordinal_position`

	return c.processSchemaRows(query)
}

func (c *Connection) processSchemaRows(query string, args ...interface{}) ([]TableSchema, error) {
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema: %w", err)
	}
	defer rows.Close()

	schemas := make([]TableSchema, 0)
	currentTable := ""
	var currentSchema *TableSchema

	for rows.Next() {
		var tableName, columnName, dataType string
		var isNullable, isPrimary bool
		var maxLength int

		if err := rows.Scan(&tableName, &columnName, &dataType, &isNullable, &isPrimary, &maxLength); err != nil {
			return nil, fmt.Errorf("failed to scan schema row: %w", err)
		}

		// If we've moved to a new table, create a new TableSchema
		if tableName != currentTable {
			if currentSchema != nil {
				schemas = append(schemas, *currentSchema)
			}
			currentTable = tableName
			currentSchema = &TableSchema{
				Name:    tableName,
				Columns: make([]ColumnSchema, 0),
				HasID:   false,
			}
		}

		column := ColumnSchema{
			Name:      columnName,
			Type:      dataType,
			IsID:      isPrimary && columnName == "id",
			Nullable:  isNullable,
			MaxLength: maxLength,
		}

		if column.IsID {
			currentSchema.HasID = true
		}

		currentSchema.Columns = append(currentSchema.Columns, column)
	}

	// Add the last table
	if currentSchema != nil {
		schemas = append(schemas, *currentSchema)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schema rows: %w", err)
	}

	return schemas, nil
}

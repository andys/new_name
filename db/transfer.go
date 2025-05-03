package db

import (
	"fmt"
	"os"
	"strings"
)

// UpsertRow handles inserting or updating a row in the destination database
func (c *Connection) UpsertRow(schema *TableSchema, data map[string]interface{}) error {
	// For tables without ID, just do a regular insert
	if !schema.HasID {
		return c.insertRow(schema, data)
	}

	// For tables with ID, do an upsert based on DB type
	switch c.Type {
	case MySQL:
		return c.mysqlUpsert(schema, data)
	case PostgreSQL:
		return c.postgresUpsert(schema, data)
	default:
		return fmt.Errorf("unsupported database type: %s", c.Type)
	}
}

func escapeIdentifier(identifier string, dbType DBType) string {
	switch dbType {
	case MySQL:
		return fmt.Sprintf("`%s`", identifier)
	case PostgreSQL:
		return fmt.Sprintf(`"%s"`, identifier)
	default:
		return identifier
	}
}

func escapeIdentifiers(identifiers []string, dbType DBType) []string {
	escaped := make([]string, len(identifiers))
	for i, id := range identifiers {
		escaped[i] = escapeIdentifier(id, dbType)
	}
	return escaped
}

func escapeUpdateClauses(clauses []string, dbType DBType) []string {
	escaped := make([]string, len(clauses))
	for i, clause := range clauses {
		parts := strings.Split(clause, " = ")
		if len(parts) == 2 {
			escaped[i] = fmt.Sprintf("%s = %s", escapeIdentifier(parts[0], dbType), parts[1])
		} else {
			escaped[i] = clause
		}
	}
	return escaped
}

func (c *Connection) insertRow(schema *TableSchema, data map[string]interface{}) error {
	if c.db == nil {
		return fmt.Errorf("sql: database is closed")
	}
	columns := make([]string, 0, len(schema.Columns))
	placeholders := make([]string, 0, len(schema.Columns))
	values := make([]interface{}, 0, len(schema.Columns))

	for _, col := range schema.Columns {
		if val, ok := data[col.Name]; ok {
			columns = append(columns, col.Name)
			values = append(values, val)
			if c.Type == PostgreSQL {
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(values)))
			} else {
				placeholders = append(placeholders, "?")
			}
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		escapeIdentifier(schema.Name, c.Type),
		strings.Join(escapeIdentifiers(columns, c.Type), ", "),
		strings.Join(placeholders, ", "),
	)

	if c.cfg.Verbose {
		fmt.Printf("Executing SQL: %s\n", query)
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if disableErr := c.DisableForeignKeyChecks(tx); disableErr != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", disableErr)
	}

	if _, err := tx.Exec(query, values...); err != nil {
		return fmt.Errorf("failed to execute query: %s, error: %w", query, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// DeleteBatchWithCount deletes rows from the given table where id is between minID and maxID and not in the provided ids.
// Returns the number of rows deleted and any error that occurred.
// ids must be a pre-sorted, non-empty slice of interface{} representing the IDs to keep.
func (c *Connection) DeleteBatchWithCount(table string, idCol string, ids []interface{}) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	minID := ids[0]
	maxID := ids[len(ids)-1]

	// Build placeholders for the NOT IN clause
	placeholders := make([]string, len(ids))
	args := []interface{}{minID, maxID}
	for i := range ids {
		placeholders[i] = "?"
		args = append(args, ids[i])
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s BETWEEN ? AND ? AND %s NOT IN (%s)",
		escapeIdentifier(table, c.Type),
		escapeIdentifier(idCol, c.Type),
		escapeIdentifier(idCol, c.Type),
		strings.Join(placeholders, ", "),
	)

	if c.cfg.Verbose {
		fmt.Printf("Executing SQL: %s\n", query)
	}

	res, err := c.GetDB().Exec(query, args...)
	if err != nil {
		if c.cfg.Debug {
			fmt.Fprintf(os.Stderr, "Error deleting from table %s: %v\n", table, err)
		}
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c *Connection) mysqlUpsert(schema *TableSchema, data map[string]interface{}) error {
	if c.db == nil {
		return fmt.Errorf("sql: database is closed")
	}
	columns := make([]string, 0, len(schema.Columns))
	placeholders := make([]string, 0, len(schema.Columns))
	values := make([]interface{}, 0, len(schema.Columns))

	for _, col := range schema.Columns {
		if val, ok := data[col.Name]; ok {
			columns = append(columns, col.Name)
			placeholders = append(placeholders, "?")
			values = append(values, val)
		}
	}

	// Only create update clauses for non-ID columns
	updateClauses := make([]string, 0)
	for _, col := range schema.Columns {
		if !col.IsID {
			if _, ok := data[col.Name]; ok {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = VALUES(%s)", escapeIdentifier(col.Name, c.Type), escapeIdentifier(col.Name, c.Type)))
			}
		}
	}

	var query string
	if len(updateClauses) > 0 {
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			escapeIdentifier(schema.Name, c.Type),
			strings.Join(escapeIdentifiers(columns, c.Type), ", "),
			strings.Join(placeholders, ", "),
			strings.Join(updateClauses, ", "),
		)
	} else {
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			schema.Name,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)
	}

	if c.cfg.Verbose {
		fmt.Printf("Executing SQL: %s\n", query)
	}
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if disableErr := c.DisableForeignKeyChecks(tx); disableErr != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", disableErr)
	}

	if _, err := tx.Exec(query, values...); err != nil {
		return fmt.Errorf("failed to execute query: %s, error: %w", query, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (c *Connection) postgresUpsert(schema *TableSchema, data map[string]interface{}) error {
	columns := make([]string, 0, len(schema.Columns))
	placeholders := make([]string, 0, len(schema.Columns))
	values := make([]interface{}, 0, len(schema.Columns))
	idColumns := make([]string, 0)

	for _, col := range schema.Columns {
		if val, ok := data[col.Name]; ok {
			columns = append(columns, col.Name)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(values)+1))
			values = append(values, val)
			if col.IsID {
				idColumns = append(idColumns, col.Name)
			}
		}
	}

	// Only create update clauses for non-ID columns
	updateClauses := make([]string, 0)
	for _, col := range schema.Columns {
		if !col.IsID {
			if _, ok := data[col.Name]; ok {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", escapeIdentifier(col.Name, c.Type), escapeIdentifier(col.Name, c.Type)))
			}
		}
	}

	var query string
	if len(updateClauses) > 0 {
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			escapeIdentifier(schema.Name, c.Type),
			strings.Join(escapeIdentifiers(columns, c.Type), ", "),
			strings.Join(placeholders, ", "),
			strings.Join(escapeIdentifiers(idColumns, c.Type), ", "),
			strings.Join(updateClauses, ", "),
		)
	} else {
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			schema.Name,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(idColumns, ", "),
		)
	}

	if c.cfg.Verbose {
		fmt.Printf("Executing SQL: %s\n", query)
	}
	if _, err := c.db.Exec(query, values...); err != nil {
		return fmt.Errorf("failed to execute query: %s, error: %w", query, err)
	}
	return nil
}
// DeleteBatch deletes rows from the given table where id is between minID and maxID and not in the provided ids.
// ids must be a pre-sorted, non-empty slice of interface{} representing the IDs to keep.
func (c *Connection) DeleteBatch(table string, idCol string, ids []interface{}) error {
	_, err := c.DeleteBatchWithCount(table, idCol, ids)
	return err
}

package db

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/andys/new_names/config"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type DBType string

const (
	MySQL      DBType = "mysql"
	PostgreSQL DBType = "postgres"
)

// Connection represents a database connection
type Connection struct {
	db   *sql.DB
	Type DBType
	cfg  *config.Config
}

// Connect establishes a database connection from a URL string
func Connect(dbURL string, cfg *config.Config, maxOpenConns int) (*Connection, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, fmt.Errorf("invalid database URL: %w", err)
	}

	var conn Connection
	var dsn string

	fmt.Printf("Connecting to %s database...\n", dbURL)

	switch u.Scheme {
	case "mysql":
		conn.Type = MySQL
		// Convert URL format to DSN format
		// Remove leading '/' from path (database name)
		database := strings.TrimPrefix(u.Path, "/")
		dsn = fmt.Sprintf("%s@tcp(%s)/%s", u.User.String(), u.Host, database)

	case "postgres", "postgresql":
		conn.Type = PostgreSQL
		// PostgreSQL can use the URL directly
		dsn = dbURL

	default:
		return nil, fmt.Errorf("unsupported database type: %s", u.Scheme)
	}

	db, err := sql.Open(string(conn.Type), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	// Set the maximum number of open connections
	db.SetMaxOpenConns(maxOpenConns)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	conn.db = db
	conn.cfg = cfg
	return &conn, nil
}

// Close closes the database connection
func (c *Connection) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// GetDB returns the underlying *sql.DB instance
func (c *Connection) GetDB() *sql.DB {
	return c.db
}

// DisableForeignKeyChecks disables foreign key constraint checking
func (c *Connection) DisableForeignKeyChecks(db *sql.Tx) error {
	var query string
	switch c.Type {
	case MySQL:
		query = "SET FOREIGN_KEY_CHECKS=0;"
	case PostgreSQL:
		query = "SET CONSTRAINTS ALL DEFERRED;"
	default:
		return fmt.Errorf("unsupported database type: %s", c.Type)
	}

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}
	return nil
}

// EnableForeignKeyChecks enables foreign key constraint checking
func (c *Connection) EnableForeignKeyChecks() error {
	var query string
	switch c.Type {
	case MySQL:
		query = "SET FOREIGN_KEY_CHECKS=1;"
	case PostgreSQL:
		query = "SET CONSTRAINTS ALL IMMEDIATE;"
	default:
		return fmt.Errorf("unsupported database type: %s", c.Type)
	}

	if _, err := c.db.Exec(query); err != nil {
		return fmt.Errorf("failed to enable foreign key checks: %w", err)
	}
	return nil
}

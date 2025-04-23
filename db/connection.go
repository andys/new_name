package db

import (
    "database/sql"
    "fmt"
    "net/url"
    "strings"

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
}

// Connect establishes a database connection from a URL string
func Connect(dbURL string) (*Connection, error) {
    u, err := url.Parse(dbURL)
    if err != nil {
        return nil, fmt.Errorf("invalid database URL: %w", err)
    }

    var conn Connection
    var dsn string

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
    if err := db.Ping(); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to ping database: %w", err)
    }

    conn.db = db
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

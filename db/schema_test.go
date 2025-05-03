package db

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/andys/new_name/config"
	"github.com/frankban/quicktest"
)

func TestGetSchema_MySQL(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	// Expect database name query
	mock.ExpectQuery("SELECT DATABASE()").
		WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("testdb"))

	// Expect schema query
	mock.ExpectQuery("FROM information_schema.TABLES").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "IS_PRIMARY", "MAX_LENGTH",
		}).
			AddRow("users", "id", "int", true, true, 0).
			AddRow("users", "name", "varchar", false, false, 255).
			AddRow("posts", "id", "int", true, true, 0).
			AddRow("posts", "title", "varchar", false, false, 100),
		)

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	schemas, err := conn.GetSchema()
	c.Assert(err, quicktest.IsNil)
	c.Assert(schemas, quicktest.HasLen, 2)
	c.Assert(schemas[0].Name, quicktest.Equals, "users")
	c.Assert(schemas[0].HasID, quicktest.IsTrue)
	c.Assert(schemas[0].Columns[0].IsID, quicktest.IsTrue)
	c.Assert(schemas[0].Columns[1].MaxLength, quicktest.Equals, 255)
	c.Assert(schemas[1].Name, quicktest.Equals, "posts")
}

func TestGetSchema_PostgreSQL(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	mock.ExpectQuery("FROM information_schema.tables").
		WillReturnRows(sqlmock.NewRows([]string{
			"table_name", "column_name", "data_type", "is_nullable", "is_primary", "max_length",
		}).
			AddRow("users", "id", "integer", true, true, 0).
			AddRow("users", "email", "varchar", false, false, 100),
		)

	conn := &Connection{db: dbMock, Type: PostgreSQL, cfg: &config.Config{}}
	schemas, err := conn.GetSchema()
	c.Assert(err, quicktest.IsNil)
	c.Assert(schemas, quicktest.HasLen, 1)
	c.Assert(schemas[0].Name, quicktest.Equals, "users")
	c.Assert(schemas[0].Columns[0].IsID, quicktest.IsTrue)
	c.Assert(schemas[0].Columns[1].MaxLength, quicktest.Equals, 100)
}

func TestProcessSchemaRows_QueryError(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	mock.ExpectQuery("bad query").WillReturnError(errors.New("fail"))
	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	_, err = conn.processSchemaRows("bad query")
	c.Assert(err, quicktest.ErrorMatches, "failed to query schema: fail")
}

func TestProcessSchemaRows_ScanError(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	rows := sqlmock.NewRows([]string{
		"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "IS_PRIMARY", "MAX_LENGTH",
	}).AddRow("users", "id", "int", true, true, 0)
	rows.RowError(0, errors.New("scan error"))
	mock.ExpectQuery("FROM information_schema.TABLES").WillReturnRows(rows)

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	_, err = conn.processSchemaRows("FROM information_schema.TABLES")
	c.Assert(err, quicktest.ErrorMatches, "error iterating schema rows: .*")
}

func TestProcessSchemaRows_RowsErr(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	rows := sqlmock.NewRows([]string{
		"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "IS_PRIMARY", "MAX_LENGTH",
	}).AddRow("users", "id", "int", true, true, 0)
	mock.ExpectQuery("FROM information_schema.TABLES").WillReturnRows(rows)
	// Simulate error after Next
	rows.RowError(0, errors.New("row error"))

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	_, err = conn.processSchemaRows("FROM information_schema.TABLES")
	c.Assert(err, quicktest.ErrorMatches, "error iterating schema rows: row error")
}

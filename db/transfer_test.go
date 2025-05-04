package db

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/andys/new_names/config"
	"github.com/frankban/quicktest"
)

func makeTestSchema(hasID bool) *TableSchema {
	return &TableSchema{
		Name:  "test_table",
		HasID: hasID,
		Columns: []ColumnSchema{
			{Name: "id", Type: "int", IsID: true},
			{Name: "name", Type: "varchar"},
		},
	}
}

func TestUpsertRow_InsertNoID(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	schema := makeTestSchema(false)
	data := map[string]interface{}{"id": 1, "name": "foo"}

	mock.ExpectBegin()
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO `test_table`").WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = conn.UpsertRow(schema, data)
	c.Assert(err, quicktest.IsNil)
}

func TestUpsertRow_UnsupportedDB(t *testing.T) {
	c := quicktest.New(t)
	conn := &Connection{Type: "sqlite", cfg: &config.Config{}}
	schema := makeTestSchema(true)
	data := map[string]interface{}{"id": 1, "name": "foo"}

	err := conn.UpsertRow(schema, data)
	c.Assert(err, quicktest.ErrorMatches, "unsupported database type: sqlite")
}

func TestInsertRow_ErrorCases(t *testing.T) {
	c := quicktest.New(t)
	dbMock, _, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	schema := makeTestSchema(false)
	data := map[string]interface{}{"id": 1, "name": "foo"}

	// Begin error
	dbMock3, mock3, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock3.Close()
	conn2 := &Connection{db: dbMock3, Type: MySQL, cfg: &config.Config{}}
	mock3.ExpectBegin().WillReturnError(fmt.Errorf("begin fail"))
	err = conn2.insertRow(schema, data)
	c.Assert(err, quicktest.ErrorMatches, "failed to begin transaction: .*begin fail")

	// Transaction error simulation
	dbMock2, mock2, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock2.Close()
	conn3 := &Connection{db: dbMock2, Type: MySQL, cfg: &config.Config{}}
	mock2.ExpectBegin()
	mock2.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock2.ExpectExec("INSERT INTO `test_table`").WithArgs(1, "foo").WillReturnError(fmt.Errorf("insert fail"))
	mock2.ExpectRollback()
	err = conn3.insertRow(schema, data)
	c.Assert(err, quicktest.ErrorMatches, "failed to execute query: .*insert fail")
}

func TestMySQLUpsert_SuccessAndNoUpdateClause(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	schema := makeTestSchema(true)
	data := map[string]interface{}{"id": 1, "name": "foo"}

	mock.ExpectBegin()
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO `test_table`").WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	err = conn.mysqlUpsert(schema, data)
	c.Assert(err, quicktest.IsNil)

	// No update clause (only ID present)
	dbMock2, mock2, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock2.Close()
	conn2 := &Connection{db: dbMock2, Type: MySQL, cfg: &config.Config{}}
	schema2 := &TableSchema{
		Name:    "test_table",
		HasID:   true,
		Columns: []ColumnSchema{{Name: "id", Type: "int", IsID: true}},
	}
	data2 := map[string]interface{}{"id": 1}
	mock2.ExpectBegin()
	mock2.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock2.ExpectExec("INSERT INTO test_table").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
	mock2.ExpectCommit()
	err = conn2.mysqlUpsert(schema2, data2)
	c.Assert(err, quicktest.IsNil)
}

func TestMySQLUpsert_ErrorCases(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	schema := makeTestSchema(true)
	data := map[string]interface{}{"id": 1, "name": "foo"}

	// Begin error
	conn2 := &Connection{db: &sql.DB{}, Type: MySQL, cfg: &config.Config{}}
	conn2.db = nil // force error
	err = conn2.mysqlUpsert(schema, data)
	c.Assert(err, quicktest.ErrorMatches, "sql: database is closed")

	// Exec error
	mock.ExpectBegin()
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO `test_table`").WithArgs(1, "foo").WillReturnError(fmt.Errorf("fail"))
	mock.ExpectRollback()
	err = conn.mysqlUpsert(schema, data)
	c.Assert(err, quicktest.ErrorMatches, "failed to execute query: .*fail")
}

func TestPostgresUpsert_SuccessAndNoUpdateClause(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: PostgreSQL, cfg: &config.Config{}}
	schema := makeTestSchema(true)
	data := map[string]interface{}{"id": 1, "name": "foo"}

	mock.ExpectExec(`INSERT INTO "test_table"`).WithArgs(1, "foo").WillReturnResult(sqlmock.NewResult(1, 1))
	err = conn.postgresUpsert(schema, data)
	c.Assert(err, quicktest.IsNil)

	// No update clause (only ID present)
	dbMock2, mock2, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock2.Close()
	conn2 := &Connection{db: dbMock2, Type: PostgreSQL, cfg: &config.Config{}}
	schema2 := &TableSchema{
		Name:    "test_table",
		HasID:   true,
		Columns: []ColumnSchema{{Name: "id", Type: "int", IsID: true}},
	}
	data2 := map[string]interface{}{"id": 1}
	mock2.ExpectExec("INSERT INTO test_table").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
	err = conn2.postgresUpsert(schema2, data2)
	c.Assert(err, quicktest.IsNil)
}

func TestPostgresUpsert_Error(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: PostgreSQL, cfg: &config.Config{}}
	schema := makeTestSchema(true)
	data := map[string]interface{}{"id": 1, "name": "foo"}

	mock.ExpectExec(`INSERT INTO "test_table"`).WithArgs(1, "foo").WillReturnError(fmt.Errorf("fail"))
	err = conn.postgresUpsert(schema, data)
	c.Assert(err, quicktest.ErrorMatches, "failed to execute query: .*fail")
}

func TestDeleteBatchWithCount_SingleID(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	table := "test_table"
	idCol := "id"
	ids := []interface{}{42}

	// Expect the correct SQL for single ID
	mock.ExpectExec("DELETE FROM `test_table` WHERE `id` > ?").
		WithArgs(42).
		WillReturnResult(sqlmock.NewResult(0, 3)) // pretend 3 rows deleted

	n, err := conn.DeleteBatchWithCount(table, idCol, ids)
	c.Assert(err, quicktest.IsNil)
	c.Assert(n, quicktest.Equals, int64(3))
}

func TestEscapeIdentifier(t *testing.T) {
	c := quicktest.New(t)
	c.Assert(escapeIdentifier("foo", MySQL), quicktest.Equals, "`foo`")
	c.Assert(escapeIdentifier("foo", PostgreSQL), quicktest.Equals, `"foo"`)
	c.Assert(escapeIdentifier("foo", "sqlite"), quicktest.Equals, "foo")
}

func TestEscapeIdentifiers(t *testing.T) {
	c := quicktest.New(t)
	ids := []string{"a", "b"}
	c.Assert(escapeIdentifiers(ids, MySQL), quicktest.DeepEquals, []string{"`a`", "`b`"})
	c.Assert(escapeIdentifiers(ids, PostgreSQL), quicktest.DeepEquals, []string{`"a"`, `"b"`})
}

func TestEscapeUpdateClauses(t *testing.T) {
	c := quicktest.New(t)
	clauses := []string{"foo = ?", "bar = ?"}
	c.Assert(escapeUpdateClauses(clauses, MySQL), quicktest.DeepEquals, []string{"`foo` = ?", "`bar` = ?"})
	c.Assert(escapeUpdateClauses(clauses, PostgreSQL), quicktest.DeepEquals, []string{`"foo" = ?`, `"bar" = ?`})
}

package db

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/andys/new_names/config"
	"github.com/frankban/quicktest"
)

func TestDisableForeignKeyChecks_MySQL(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	mock.ExpectBegin()
	tx, err := dbMock.Begin()
	c.Assert(err, quicktest.IsNil)
	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}

	mock.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(1, 1))
	err = conn.DisableForeignKeyChecks(tx)
	c.Assert(err, quicktest.IsNil)
}

func TestDisableForeignKeyChecks_PostgreSQL(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	mock.ExpectBegin()
	tx, err := dbMock.Begin()
	c.Assert(err, quicktest.IsNil)
	conn := &Connection{db: dbMock, Type: PostgreSQL, cfg: &config.Config{}}

	mock.ExpectExec("SET CONSTRAINTS ALL DEFERRED;").WillReturnResult(sqlmock.NewResult(1, 1))
	err = conn.DisableForeignKeyChecks(tx)
	c.Assert(err, quicktest.IsNil)
}

func TestEnableForeignKeyChecks_MySQL(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS=1;").WillReturnResult(sqlmock.NewResult(1, 1))
	err = conn.EnableForeignKeyChecks()
	c.Assert(err, quicktest.IsNil)
}

func TestEnableForeignKeyChecks_PostgreSQL(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: PostgreSQL, cfg: &config.Config{}}
	mock.ExpectExec("SET CONSTRAINTS ALL IMMEDIATE;").WillReturnResult(sqlmock.NewResult(1, 1))
	err = conn.EnableForeignKeyChecks()
	c.Assert(err, quicktest.IsNil)
}

func TestInsertRow(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	schema := &TableSchema{
		Name: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
		},
	}
	data := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	mock.ExpectBegin()
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO `users`").WithArgs(1, "test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = conn.insertRow(schema, data)
	c.Assert(err, quicktest.IsNil)
}

func TestMySQLUpsert(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}
	schema := &TableSchema{
		Name:  "users",
		HasID: true,
		Columns: []ColumnSchema{
			{Name: "id", Type: "int", IsID: true},
			{Name: "name", Type: "varchar"},
		},
	}
	data := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	mock.ExpectBegin()
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS=0;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO `users`").WithArgs(1, "test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = conn.mysqlUpsert(schema, data)
	c.Assert(err, quicktest.IsNil)
}

func TestPostgresUpsert(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: PostgreSQL, cfg: &config.Config{}}
	schema := &TableSchema{
		Name:  "users",
		HasID: true,
		Columns: []ColumnSchema{
			{Name: "id", Type: "int", IsID: true},
			{Name: "name", Type: "varchar"},
		},
	}
	data := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	mock.ExpectExec(`INSERT INTO "users"`).WithArgs(1, "test").WillReturnResult(sqlmock.NewResult(1, 1))

	err = conn.postgresUpsert(schema, data)
	c.Assert(err, quicktest.IsNil)
}

func TestDeleteBatch(t *testing.T) {
	c := quicktest.New(t)
	dbMock, mock, err := sqlmock.New()
	c.Assert(err, quicktest.IsNil)
	defer dbMock.Close()

	conn := &Connection{db: dbMock, Type: MySQL, cfg: &config.Config{}}

	table := "users"
	idCol := "id"
	ids := []interface{}{1, 2, 3}

	// The query should match the generated SQL in DeleteBatch
	mock.ExpectExec("DELETE FROM `users` WHERE `id` BETWEEN \\? AND \\? AND `id` NOT IN \\(\\?, \\?, \\?\\)").
		WithArgs(1, 3, 1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))

	err = conn.DeleteBatch(table, idCol, ids)
	c.Assert(err, quicktest.IsNil)
}

func TestDeleteBatch_EmptyIDs(t *testing.T) {
	c := quicktest.New(t)
	conn := &Connection{db: nil, Type: MySQL, cfg: &config.Config{}}
	// Should do nothing and return nil
	err := conn.DeleteBatch("users", "id", []interface{}{})
	c.Assert(err, quicktest.IsNil)
}

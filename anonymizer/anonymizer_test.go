package anonymizer

import (
	"testing"

	"github.com/andys/new_names/config"
	"github.com/andys/new_names/db"
	"github.com/frankban/quicktest"
)

func TestAnonymize_ReplacesFields(t *testing.T) {
	c := quicktest.New(t)
	schema := &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnSchema{
			{Name: "email", Type: "varchar", MaxLength: 100},
			{Name: "name", Type: "varchar", MaxLength: 50},
			{Name: "id", Type: "int", IsID: true},
		},
	}
	row := &Row{
		Schema: schema,
		Data: map[string]interface{}{
			"email": "real@email.com",
			"name":  "Real Name",
			"id":    123,
		},
	}
	cfg := &config.Config{
		AnonymizeFields: map[string][]string{
			"users": {"email", "name"},
		},
	}

	Anonymize(row, cfg)

	c.Assert(row.Data["email"], quicktest.Not(quicktest.Equals), "real@email.com")
	c.Assert(row.Data["name"], quicktest.Not(quicktest.Equals), "Real Name")
	c.Assert(row.Data["id"], quicktest.Equals, 123)
}

func TestAnonymize_HandlesNilValues(t *testing.T) {
	c := quicktest.New(t)
	schema := &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnSchema{
			{Name: "email", Type: "varchar", MaxLength: 100},
		},
	}
	row := &Row{
		Schema: schema,
		Data: map[string]interface{}{
			"email": nil,
		},
	}
	cfg := &config.Config{
		AnonymizeFields: map[string][]string{
			"users": {"email"},
		},
	}

	Anonymize(row, cfg)

	c.Assert(row.Data["email"], quicktest.IsNil)
}

func TestAnonymize_HandlesEmptyStrings(t *testing.T) {
	c := quicktest.New(t)
	schema := &db.TableSchema{
		Name: "users",
		Columns: []db.ColumnSchema{
			{Name: "email", Type: "varchar", MaxLength: 100},
		},
	}
	row := &Row{
		Schema: schema,
		Data: map[string]interface{}{
			"email": "",
		},
	}
	cfg := &config.Config{
		AnonymizeFields: map[string][]string{
			"users": {"email"},
		},
	}

	Anonymize(row, cfg)

	c.Assert(row.Data["email"], quicktest.Equals, "")
}

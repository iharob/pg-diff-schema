package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"utils"
)

type Schema struct {
	tables    []*Table
	sequences []*Sequence
	types     []*Type
	name      string
}

const GetSequences string = "SELECT sequencename FROM pg_catalog.pg_sequences WHERE schemaname = $1"

func (schema *Schema) collectConstraints(db *sql.DB) error {
	var err error
	// Second pass now also get relations
	for _, table := range schema.tables {
		if table.kind == BaseTable {
			table.constraints, err = getConstraints(db, *table, *schema)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func isSequenceInArray(array []*Sequence, value *Sequence) bool {
	for _, item := range array {
		if value.name == item.name {
			return true
		}
	}
	return false
}

func (schema *Schema) sequenceSetDifference(other *Schema) ([]*Sequence, error) {
	var sequences []*Sequence
	for _, item := range schema.sequences {
		if !isSequenceInArray(other.sequences, item) {
			sequences = append(sequences, item)
		}
	}
	return sequences, nil
}

func (schema *Schema) typeSetDifference(other *Schema) ([]*Type, error) {
	var types []*Type
	for _, item := range schema.types {
		var found *Type
		found = other.FindTypeByName(item.name)
		if found == nil {
			types = append(types, item)
		}
	}
	return types, nil
}

func (schema *Schema) tableSetIntersection(other *Schema) ([]*Table, error) {
	var tables []*Table
	for _, table := range schema.tables {
		var found *Table
		found = other.FindTableByName(table.name)
		if found != nil {
			tables = append(tables, table)
		}
	}
	return tables, nil
}

func (schema *Schema) tableSetDifference(other *Schema) ([]*Table, error) {
	var tables []*Table
	for _, table := range schema.tables {
		var found *Table
		found = other.FindTableByName(table.name)
		if found == nil || found.kind == View {
			tables = append(tables, table)
		}
	}
	return tables, nil
}

func (schema *Schema) collectTypes(db *sql.DB, schemaName string) error {
	var rows *sql.Rows
	var err error
	var array []byte
	// First list the tables
	if rows, err = db.Query(GetTypes, schemaName); err != nil {
		return err
	}
	for rows.Next() {
		var item Type
		if err = rows.Scan(
			&item.name,
			&array,
			&item.isEnum,
		); err != nil {
			return err
		}
		if !item.isEnum {
			panic("item is not enum")
		}
		if err = utils.ParseArray(array, &item.values); err != nil {
			return err
		}
		schema.types = append(schema.types, &item)
	}
	return nil
}

func (schema *Schema) findSequence(name string) *Sequence {
	for _, table := range schema.tables {
		for _, column := range table.columns {
			var defaultValue interface{}
			defaultValue = column.defaultValue
			switch sequence := defaultValue.(type) {
			case *Sequence:
				if sequence.name == name {
					return sequence
				}
				break
			}
		}
	}
	return nil
}

func (schema *Schema) collectSequences(db *sql.DB) error {
	var rows *sql.Rows
	var err error
	if rows, err = db.Query(GetSequences, "public"); err != nil {
		return err
	}
	for rows.Next() {
		var sequence *Sequence
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return err
		}
		sequence = schema.findSequence(name)
		if sequence != nil {
			schema.sequences = append(schema.sequences, sequence)
		}
	}
	return nil
}

func removeSemicolon(statement string) string {
	return strings.Trim(statement, ";")
}

func (schema *Schema) collectTables(db *sql.DB, catalog string, schemaName string) error {
	var rows *sql.Rows
	var viewDefinition sql.NullString
	var err error
	// First list the tables
	if rows, err = db.Query(GetTables, catalog, schemaName); err != nil {
		return err
	}
	for rows.Next() {
		var table Table
		if err = rows.Scan(
			&table.name,
			&table.kind,
			&table.schema,
			&table.catalog,
			&viewDefinition,
		); err != nil {
			return err
		}
		if viewDefinition.Valid {
			table.viewDefinition = removeSemicolon(viewDefinition.String)
		}
		schema.tables = append(schema.tables, &table)
	}
	return nil
}

func (schema *Schema) collectColumns(db *sql.DB) error {
	var err error
	for _, table := range schema.tables {
		if err = table.collectColumns(db); err != nil {
			return err
		}
	}
	return nil
}

func (schema *Schema) examineIntersectingTables(target *Schema) (string, error) {
	var tables []*Table
	var err error
	var builder strings.Builder
	var tmp string
	if tables, err = schema.tableSetIntersection(target); err != nil {
		return "", err
	}
	for _, table := range tables {
		var found *Table
		found = target.FindTableByName(table.name)
		if found == nil {
			return "", fmt.Errorf("table `%s' not found in target schema", table.name)
		}
		if tmp, err = table.Diff(found); err != nil {
			return "", err
		}
		builder.WriteString(tmp)
	}
	return builder.String(), nil
}

func (schema *Schema) generateNeededCreateTableStatements(target *Schema) (string, error) {
	var tables []*Table
	var builder strings.Builder
	var err error
	if tables, err = schema.tableSetDifference(target); err != nil {
		return "", err
	}
	for _, table := range tables {
		builder.WriteString(table.CreateStatement())
	}
	return builder.String(), nil
}

func (schema *Schema) generateNeededDropTableStatements(target *Schema) (string, error) {
	var tables []*Table
	var builder strings.Builder
	var err error
	if tables, err = target.tableSetDifference(schema); err != nil {
		return "", err
	}
	for _, table := range tables {
		builder.WriteString(table.DropStatement())
	}
	return builder.String(), nil
}

func (schema *Schema) generateNeededCreateSequenceStatements(target *Schema) (string, error) {
	var sequences []*Sequence
	var builder strings.Builder
	var err error
	if sequences, err = schema.sequenceSetDifference(target); err != nil {
		return "", err
	}
	for _, item := range sequences {
		builder.WriteString(fmt.Sprintf("CREATE SEQUENCE \"%s\";\n", item))
	}
	return builder.String(), nil
}

func (schema *Schema) generateNeededCreateTypeStatements(target *Schema) (string, error) {
	var types []*Type
	var builder strings.Builder
	var err error
	if types, err = schema.typeSetDifference(target); err != nil {
		return "", err
	}
	for _, item := range types {
		builder.WriteString(item.CreateStatement())
	}
	return builder.String(), nil
}

func (schema *Schema) generateNeededDropTypeStatements(target *Schema) (string, error) {
	var types []*Type
	var builder strings.Builder
	var err error
	if types, err = target.typeSetDifference(schema); err != nil {
		return "", err
	}
	for _, item := range types {
		builder.WriteString(item.DropStatement())
	}
	return builder.String(), nil
}

func (schema *Schema) FindTypeByName(name string) *Type {
	for _, item := range schema.types {
		if strings.Compare(item.name, name) == 0 {
			return item
		}
	}
	return nil
}

func (schema *Schema) FindTableByName(name string) *Table {
	for _, table := range schema.tables {
		if strings.Compare(table.name, name) == 0 {
			return table
		}
	}
	return nil
}

func (schema *Schema) Diff(target *Schema) (string, error) {
	var err error
	var builder strings.Builder
	var tmp string
	if tmp, err = schema.generateNeededCreateSequenceStatements(target); err != nil {
		return "", err
	}
	builder.WriteString(tmp)
	if tmp, err = schema.generateNeededCreateTypeStatements(target); err != nil {
		return "", err
	}
	builder.WriteString(tmp)
	if tmp, err = schema.generateNeededDropTypeStatements(target); err != nil {
		return "", err
	}
	builder.WriteString(tmp)
	if tmp, err = schema.examineIntersectingTables(target); err != nil {
		return "", err
	}
	builder.WriteString(tmp)
	if tmp, err = schema.generateNeededDropTableStatements(target); err != nil {
		return "", err
	}
	builder.WriteString(tmp)
	if tmp, err = schema.generateNeededCreateTableStatements(target); err != nil {
		return "", err
	}
	builder.WriteString(tmp)
	return builder.String(), nil
}

func buildSchema(db *sql.DB, catalog string, schemaName string) (*Schema, error) {
	var schema Schema
	var err error
	if err = schema.collectTypes(db, schemaName); err != nil {
		return nil, err
	}
	if err = schema.collectTables(db, catalog, schemaName); err != nil {
		return nil, err
	}
	// First pass simply list columns
	if err = schema.collectColumns(db); err != nil {
		return nil, err
	}
	// First pass simply list columns
	if err = schema.collectConstraints(db); err != nil {
		return nil, err
	}
	if err = schema.collectSequences(db); err != nil {
		return nil, err
	}
	return &schema, nil
}

func NewSchema(host string, port int16, user string, pass string, name string) (*Schema, error) {
	var dsn string = fmt.Sprintf(DsnBase, host, port, user, name, pass)
	var db *sql.DB
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Println(err)
			return
		}
	}()
	return buildSchema(db, name, "public")
}

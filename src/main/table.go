package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

const GetTables string = `
SELECT
  information_schema.tables.table_name,
  information_schema.tables.table_type,
  information_schema.tables.table_schema,
  information_schema.tables.table_catalog,
  information_schema.views.view_definition
FROM information_schema.tables
NATURAL LEFT JOIN information_schema.views
WHERE information_schema.tables.table_catalog = $1 AND
  information_schema.tables.table_schema = $2
`

type TableType string

const (
	BaseTable TableType = "BASE TABLE"
	View                = "VIEW"
)

type Table struct {
	name           string
	constraints    []*Constraint
	columns        []*Column
	kind           TableType
	schema         string
	catalog        string
	viewDefinition string
}

func (table *Table) FindColumn(search *Column) *Column {
	for _, column := range table.columns {
		if column.name != search.name {
			continue
		}
		return column
	}
	return nil
}

func (table *Table) FindColumnByPosition(position int) *Column {
	if table == nil {
		return nil
	}
	for _, column := range table.columns {
		if column.position == position {
			return column
		}
	}
	return nil
}

func (table Table) String() string {
	var builder strings.Builder
	// write the header
	builder.WriteString(fmt.Sprintf("%s\n", table.name))
	for _, column := range table.columns {
		builder.WriteString(fmt.Sprintf("\t%s\n", column))
	}
	return builder.String()
}

func (table *Table) columnSetDifference(other *Table) ([]*Column, error) {
	var difference []*Column
	for _, column := range table.columns {
		var found *Column
		found = other.FindColumn(column)
		if found == nil {
			difference = append(difference, column)
		}
	}
	return difference, nil
}

func (table *Table) GetPrimaryKey() *Constraint {
	for _, constraint := range table.constraints {
		if constraint.kind == PrimaryKey {
			return constraint
		}
	}
	return nil
}

func (table *Table) constraintSetDifference(target *Table) ([]*Constraint, error) {
	var constraints []*Constraint
	var err error
	for _, constraint := range table.constraints {
		var found bool
		found, err = target.hasConstraint(constraint)
		if err != nil {
			return nil, err
		} else if !found {
			constraints = append(constraints, constraint)
		}
	}
	return constraints, nil
}

func (table *Table) collectColumns(db *sql.DB) error {
	var rows *sql.Rows
	var defaultValue interface{}
	var err error
	var sequence *Sequence
	// First list the columns
	rows, err = db.Query(GetColumns, table.name, table.catalog, table.schema)
	if err != nil {
		return err
	}
	for rows.Next() {
		var nullable string
		var column *Column = &Column{}
		err = rows.Scan(
			&column.name,
			&column.position,
			&defaultValue,
			&nullable,
			&column.dataType,
			&column.length,
			&column.numericPrecision,
			&column.numericScale,
		)
		if err != nil {
			return err
		}
		column.isNullable = nullable != "NO"
		if defaultValue != nil {
			sequence = getSequenceIfAny(defaultValue.(string), column)
			if sequence == nil {
				column.defaultValue = defaultValue
			} else {
				column.defaultValue = sequence
			}
		}
		column.table = table
		// Append to the array now that it's good
		table.columns = append(table.columns, column)
	}
	return nil
}

func getSequenceIfAny(value string, column *Column) *Sequence {
	var list [][]string
	var re *regexp.Regexp
	var sequence Sequence
	re = regexp.MustCompile("nextval\\('\"([^\"]+)\"'::regclass\\)")
	list = re.FindAllStringSubmatch(value, -1)
	if len(list) == 1 {
		sequence.column = column
		sequence.name = list[0][1]
	} else {
		return nil
	}
	return &sequence
}

func (table *Table) hasConstraint(target *Constraint) (bool, error) {
	for _, constraint := range table.constraints {
		var equals bool
		var err error
		equals, err = constraint.Equal(target)
		if err != nil {
			return false, err
		} else if equals {
			return true, nil
		}
	}
	return false, nil
}

func (table *Table) DropStatement() string {
	if table.kind == View {
		return fmt.Sprintf("DROP VIEW IF EXISTS \"%s\" CASCADE;\n", table.name)
	} else {
		return fmt.Sprintf("DROP TABLE IF EXISTS \"%s\";\n", table.name)
	}
}

func (table *Table) CreateStatement() string {
	if table.kind == View {
		return fmt.Sprintf("CREATE OR REPLACE VIEW \"%s\" AS (\n  %s\n);\n", table.name, table.viewDefinition)
	} else {
		var list []string
		for _, column := range table.columns {
			list = append(list, column.String())
		}
		// Add the constraints
		for _, constraint := range table.constraints {
			list = append(list, constraint.String())
		}
		return fmt.Sprintf("CREATE TABLE \"%s\" (\n  %s\n);\n", table.name, strings.Join(list, ",\n  "))
	}
}

func (table *Table) AddColumnStatement(column *Column) string {
	return fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN %v;\n", table.name, column)
}

func (table *Table) DropColumnStatement(column *Column) string {
	return fmt.Sprintf("ALTER TABLE \"%s\" DROP COLUMN IF EXISTS \"%s\";\n", table.name, column.name)
}

func (table *Table) columnDiff(target *Table) (string, error) {
	var builder strings.Builder
	for _, column := range target.columns {
		var result string
		var err error
		var other *Column
		other = table.FindColumn(column)
		if other == nil {
			// The entire column does not exist
			return "", nil
		}
		result, err = column.Diff(other)
		if err != nil {
			return "", err
		}
		builder.WriteString(result)
	}
	return builder.String(), nil
}

func (table *Table) Diff(target *Table) (string, error) {
	var err error
	var constraints []*Constraint
	var columns []*Column
	var tmp string
	// var moved bool
	var builder strings.Builder
	if table.kind != target.kind {
		return "", fmt.Errorf("source table `%s' is not of the same kind as the target table", table.name)
	}
	if table.kind == View {
		return "", nil
	}
	// Generate add column for new/columns columns
	if columns, err = table.columnSetDifference(target); err != nil {
		return "", err
	}
	for _, column := range columns {
		builder.WriteString(table.AddColumnStatement(column))
	}
	if tmp, err = table.columnDiff(target); err != nil {
		return "", err
	}
	builder.WriteString(tmp)
	if constraints, err = table.constraintSetDifference(target); err != nil {
		return "", err
	}
	for _, constraint := range constraints {
		// FIXME: generate constraint creation code
		builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" %s;\n", table.name, constraint.name, constraint))
	}
	if constraints, err = target.constraintSetDifference(table); err != nil {
		return "", err
	}
	for _, constraint := range constraints {
		builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" DROP CONSTRAINT IF EXISTS \"%s\";\n", table.name, constraint.name))
	}
	// Generate drop obsolete columns
	if columns, err = target.columnSetDifference(table); err != nil {
		return "", err
	}
	for _, column := range columns {
		builder.WriteString(table.DropColumnStatement(column))
	}
	// Add new/missing constraints
	if constraints, err = table.constraintSetDifference(target); err != nil {
		return "", err
	}
	return builder.String(), nil
}

func (table *Table) Equal(other *Table) bool {
	return strings.Compare(table.name, other.name) == 0
}

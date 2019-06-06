package main

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const GetTables string = `
SELECT
  table_name,
  table_type,
  table_schema,
  table_catalog
FROM information_schema.tables
WHERE table_catalog = $1 AND
  table_schema = $2
`

type TableType string

const (
	BaseTable TableType = "BASE TABLE"
	View                = "VIEW"
)

type Table struct {
	name        string
	constraints []*Constraint
	columns     []*Column
	kind        TableType
	schema      string
	catalog     string
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
	return fmt.Sprintf("DROP TABLE IF EXISTS \"%s\";\n", table.name)
}

func (table *Table) CreateStatement() string {
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

func (table *Table) AddColumnStatement(column *Column) string {
	return fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN %v;\n", table.name, column)
}

func (table *Table) DropColumnStatement(column *Column) string {
	return fmt.Sprintf("ALTER TABLE \"%s\" DROP COLUMN IF EXISTS \"%s\";\n", table.name, column.name)
}

func (table *Table) columnDiff(target *Table) (string, error) {
	var defaultValue interface{}
	var otherDefaultValue interface{}
	var builder strings.Builder
	for _, column := range target.columns {
		var other *Column = table.FindColumn(column)
		var table *Table = column.table
		if other.dataType != column.dataType || other.length != column.length {
			builder.WriteString(
				fmt.Sprintf(
					"ALTER TABLE \"%s\" ALTER COLUMN \"%s\" TYPE %s USING \"%s\"::%s;\n",
					table.name,
					column.name,
					other.GetTypeString(),
					column.name,
					other.dataType,
				),
			)
		}
		defaultValue = column.defaultValue
		otherDefaultValue = other.defaultValue
		switch value := defaultValue.(type) {
		case nil:
			if other.defaultValue != nil {
				var str, err = other.GetDefaultValue()
				if err == nil {
					builder.WriteString(
						fmt.Sprintf(
							"ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET DEFAULT %s;\n",
							table.name,
							column.name,
							str,
						),
					)
				}
			}
		case *Sequence:
			switch otherValue := otherDefaultValue.(type) {
			case nil:
				builder.WriteString(fmt.Sprintf("DROP SEQUENCE IF EXISTS \"%s\";\n", value.name))
				break
			case *Sequence:
				if value.name != otherValue.name {
					builder.WriteString(fmt.Sprintf("DROP SEQUENCE IF EXISTS \"%s\";\n", otherValue.name))
					builder.WriteString(
						fmt.Sprintf(
							"ALTER SEQUENCE \"%s\" RENAME TO \"%s\";\n",
							value.name,
							otherValue.name,
						),
					)
				}
			}
			break
		case string:
			if value != other.defaultValue {
				fmt.Printf("%s -> %s\n", other.defaultValue, value)
				builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP DEFAULT;\n", table.name, column.name))
			}
			break
		}
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
	// Handle columns that are not in the right position, since this is VERY HARD
	// we are just issuing a note about it, in future versions we can attempt to
	// actually implement moving the columns.
	//
	// This requires a huge effort to avoid inconsistencies since there are only a
	// few solutions because PostgreSQL does not support moving columns.
	//
	// Since this isn't really implemented in PostgreSQL we encourage the user
	// of this program not to rely on column positions in their queries
	/*moved, err = target.findAndUpdateMovedColumns(table.columns)
	if err != nil {
		return "", err
	}
	if table.kind == BaseTable && moved {
		var tmpName string
		var columns []string
		for _, column := range table.columns {
			columns = append(columns, fmt.Sprintf("\"%s\"::%s", column.name, column.dataType))
		}
		tmpName = "__replacing_table__"
		builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" DISABLE TRIGGER ALL;\n", table.name))
		builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" RENAME TO \"%s\";\n", table.name, tmpName))
		builder.WriteString(table.CreateStatement());
		builder.WriteString(fmt.Sprintf("INSERT INTO \"%s\" SELECT %s FROM \"%s\";\n", table.name, strings.Join(columns, ", "), tmpName))
		builder.WriteString(fmt.Sprintf("DROP TABLE \"%s\" CASCADE;\n", table.name))
		builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" ENABLE TRIGGER ALL;\n", table.name))
	}*/
	// Drop obsolete constraints
	if constraints, err = target.constraintSetDifference(table); err != nil {
		return "", err
	}
	for _, constraint := range constraints {
		// FIXME: generate constraint creation code
		builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\"%s;\n", table.name, constraint.name, constraint))
	}
	for _, constraint := range constraints {
		builder.WriteString(fmt.Sprintf("ALTER TABLE \"%s\" DROP CONSTRAINT IF EXISTS \"%s\";\n", table.name, constraint.name))
	}
	// Generate drop obsolete columns
	if columns, err = target.columnSetDifference(table); err != nil {
		return "", err
	}
	for _, column := range columns {
		if table.kind == BaseTable {
			builder.WriteString(table.DropColumnStatement(column))
		} else if table.kind == View {
			return "", errors.New("you have to recreate this view, it's broken")
		}
	}
	// If it's a view we don't do any constraint check
	if table.kind == View {
		return builder.String(), nil
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

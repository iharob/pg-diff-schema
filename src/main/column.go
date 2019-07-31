package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const GetColumns string = `
SELECT
  column_name,
  ordinal_position,
  column_default,
  is_nullable,
  quote_ident(udt_name),
  character_maximum_length,
  numeric_precision,
  numeric_scale
FROM
  information_schema.columns
WHERE
  table_name = $1 AND 
  table_catalog = $2 AND 
  table_schema = $3
`

type Column struct {
	name             string
	position         int
	defaultValue     interface{}
	isNullable       bool
	dataType         string
	length           sql.NullInt64
	table            *Table
	constraints      []*Constraint
	from             int
	isAutoincrement  bool
	numericPrecision sql.NullInt64
	numericScale     sql.NullInt64
}

func (column *Column) GetTypeString() string {
	var length sql.NullInt64
	var code strings.Builder
	// The base is always the same
	code.WriteString(fmt.Sprintf("%s", column.dataType))
	length = column.length
	if length.Valid {
		code.WriteString(fmt.Sprintf("(%d)", length.Int64))
	}
	return code.String()
}

func (column *Column) GetDefaultValue() (string, error) {
	var defaultValue interface{}
	defaultValue = column.defaultValue
	switch value := defaultValue.(type) {
	case *Sequence:
		return fmt.Sprintf("NEXTVAL('\"%s\"')", value.name), nil
	case string:
		return value, nil
	}
	return "", errors.New("no default value")
}

func (column Column) String() string {
	var code strings.Builder
	var err error
	var defaultValue string
	code.WriteString(fmt.Sprintf("\"%s\" %s", column.name, column.GetTypeString()))
	defaultValue, err = column.GetDefaultValue()
	if err == nil {
		code.WriteString(fmt.Sprintf(" DEFAULT %s", defaultValue))
	}
	if !column.isNullable {
		code.WriteString(" NOT NULL")
	}
	return code.String()
}

func (column *Column) Diff(target *Column) (string, error) {
	var defaultValue interface{}
	var otherDefaultValue interface{}
	var table *Table
	var builder strings.Builder
	table = column.table
	if column.isNullable && !target.isNullable {
		builder.WriteString(
			fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL;\n", table.name, column.name),
		)
	} else if !column.isNullable && target.isNullable {
		builder.WriteString(
			fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP NOT NULL;\n", table.name, column.name),
		)
	}
	if column.dataType == "\"numeric\"" && differentPrecisionOrScale(target, column) {
	} else if target.dataType != column.dataType || target.length != column.length {
		builder.WriteString(
			fmt.Sprintf(
				"ALTER TABLE \"%s\" ALTER COLUMN \"%s\" TYPE %s USING \"%s\"::%s;\n",
				table.name,
				column.name,
				target.GetTypeString(),
				column.name,
				target.dataType,
			),
		)
	}
	defaultValue = column.defaultValue
	otherDefaultValue = target.defaultValue
	switch value := defaultValue.(type) {
	case nil:
		if target.defaultValue != nil {
			var str, err = target.GetDefaultValue()
			if err != nil {
				return "", err
			}
			builder.WriteString(
				fmt.Sprintf(
					"ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET DEFAULT %s;\n",
					table.name,
					column.name,
					str,
				),
			)
		}
	case *Sequence:
		switch otherValue := otherDefaultValue.(type) {
		case nil:
			builder.WriteString(fmt.Sprintf("DROP SEQUENCE IF EXISTS \"%s\";\n", value.name))
			break
		case *Sequence:
			if value.name != otherValue.name {
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
		if value != target.defaultValue {
			builder.WriteString(
				fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP DEFAULT;\n", table.name, column.name),
			)
		}
		break
	}
	return builder.String(), nil
}

func nullIntAreEqual(a sql.NullInt64, b sql.NullInt64) bool {
	if !a.Valid && !b.Valid {
		return true
	}
	return a.Int64 == b.Int64
}

func differentPrecisionOrScale(a *Column, b *Column) bool {
	return nullIntAreEqual(a.numericPrecision, b.numericPrecision) && nullIntAreEqual(a.numericScale, b.numericScale)
}

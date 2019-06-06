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
  character_maximum_length
FROM
  information_schema.columns
WHERE
  table_name = $1 AND 
  table_catalog = $2 AND 
  table_schema = $3
`

type Column struct {
	name            string
	position        int
	defaultValue    interface{}
	isNullable      bool
	dataType        string
	length          sql.NullInt64
	table           *Table
	constraints     []*Constraint
	from            int
	isAutoincrement bool
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

package main

import (
	"database/sql"
	"fmt"
	"strings"
)

const GET_COLUMNS string = `
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
	name         string
	position     int
	defaultValue interface{}
	isNullable   bool
	dataType     string
	length       sql.NullInt64
	table        *Table
	constraints  []*Constraint
	from         int
}

func (column Column) String() string {
	var length sql.NullInt64
	var code strings.Builder
	code.WriteString(fmt.Sprintf("\"%s\" %s", column.name, column.dataType))
	length = column.length
	if length.Valid {
		code.WriteString(fmt.Sprintf("(%d)", length.Int64))
	}
	if !column.isNullable {
		code.WriteString(" NOT NULL")
	}
	return code.String()
}

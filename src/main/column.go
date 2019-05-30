package main

import (
	"database/sql"
	"fmt"
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
	maxLength    sql.NullInt64
	table        *Table
	constraints  []*Constraint
	from         int
}

func (column Column) String() string {
	var nullable string
	var length string
	if !column.isNullable {
		nullable = " NOT NULL"
	}
	if column.maxLength.Valid {
		var nullInt64 sql.NullInt64 = column.maxLength
		length = fmt.Sprintf("(%d)", nullInt64.Int64)
	}
	return fmt.Sprintf(
		"\"%s\" %s%s%s",
		column.name,
		column.dataType,
		length,
		nullable,
	)
}
package main

import (
	"fmt"
	"strings"
)

const GET_TYPES = `
SELECT t.typname               AS name,
       ARRAY_AGG(e.enumlabel)  AS values,
       e.enumtypid IS NOT NULL AS is_enum
FROM pg_catalog.pg_type t
       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
       LEFT JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_type_is_visible(t.oid)
  AND n.nspname = $1
GROUP BY t.typname, e.enumtypid
`

type Type struct {
	name   string
	isEnum bool
	values []string
	oid    int
}

func (item *Type) DropStatement() string {
	return fmt.Sprintf("DROP TYPE \"%s\";\n", item.name)
}

func (item *Type) CreateStatement() string {
	var builder strings.Builder
	builder.WriteString("CREATE TYPE \"")
	builder.WriteString(item.name)
	builder.WriteString("\"")
	if item.isEnum {
		builder.WriteString(" AS ENUM ('")
		builder.WriteString(strings.Join(item.values, "', '"))
	} else {
		return fmt.Sprintf("-- \033[31mWARNING\033[0m: no idea how to create this type -> %s\n", item.name)
	}
	builder.WriteString("');\n")
	return builder.String()
}

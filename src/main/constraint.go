package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"alasimi.com/pg-diff-schema/src/utils"
)

type ConstraintType string
type Constraint struct {
	name         string
	kind         ConstraintType
	table        *Table
	foreignTable *Table
	keys         []*Column
	foreignKeys  []*Column
}

const GetConstraints string = `
SELECT 
       conname, 
       contype, 
       btrim(conrelid::regclass::text, '"'), 
       btrim(confrelid::regclass::text, '"'), 
       conkey, 
       confkey 
FROM pg_constraint 
WHERE conrelid::regclass = quote_ident($1)::regclass
`

type stringArray []string

const (
	Unique     ConstraintType = "u"
	PrimaryKey                = "p"
	ForeignKey                = "f"
	Check                     = "c"
)

func (array *stringArray) Scan(src interface{}) error {
	switch value := src.(type) {
	case []byte:
		return utils.ParseArray(value, (*[]string)(array))
	case nil:
		break
	default:
		return fmt.Errorf("invalid input type for string array %t", src)
	}
	return nil
}

func getAllKeys(keys stringArray, table *Table) ([]*Column, error) {
	var columns []*Column
	for _, key := range keys {
		var column *Column
		var err error
		var position int
		position, err = strconv.Atoi(key)
		if err != nil {
			return nil, err
		}
		column = table.FindColumnByPosition(position)
		if column == nil {
			return nil, nil // fmt.Errorf("column `%d' not found in table `%s'", position, table.name)
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func getConstraints(db *sql.DB, table Table, schema Schema) ([]*Constraint, error) {
	var rows *sql.Rows
	var constraints []*Constraint
	var err error
	// First list the constraints
	rows, err = db.Query(GetConstraints, table.name)
	if err != nil {
		return nil, err
	}
	constraints = make([]*Constraint, 0)
	for rows.Next() {
		var constraint Constraint
		var tableName string
		var foreignTableName string
		var keys stringArray
		var foreignKeys stringArray
		if err = rows.Scan(
			&constraint.name,
			&constraint.kind,
			&tableName,
			&foreignTableName,
			&keys,
			&foreignKeys,
		); err != nil {
			return nil, err
		}
		constraint.table = schema.FindTableByName(tableName)
		constraint.foreignTable = schema.FindTableByName(foreignTableName)
		constraint.keys, err = getAllKeys(keys, constraint.table)
		if err != nil {
			return nil, err
		}
		for _, column := range constraint.keys {
			column.constraints = append(column.constraints, &constraint)
		}
		constraint.foreignKeys, err = getAllKeys(foreignKeys, constraint.foreignTable)
		if err != nil {
			return nil, err
		}
		/*for _, column := range constraint.foreignKeys {
			column.constraints = append(column.constraints, &constraint)
		}*/
		constraints = append(constraints, &constraint)
	}
	return constraints, nil
}

func isColumnInArray(array []*Column, which *Column) bool {
	var table *Table
	table = which.table
	for _, item := range array {
		var namesMatch bool
		var tablesMatch bool
		var typesMatch bool
		namesMatch = strings.Compare(item.name, which.name) == 0
		if !namesMatch {
			continue
		}
		typesMatch = strings.Compare(item.dataType, which.dataType) == 0
		if !typesMatch {
			continue
		}
		tablesMatch = table.Equal(item.table)
		if !tablesMatch {
			continue
		}
		return true
	}
	return false
}

func compareKeys(first []*Column, second []*Column) bool {
	for _, key := range first {
		if !isColumnInArray(second, key) {
			return false
		}
	}
	return true
}

func (constraint *Constraint) keysEqual(target *Constraint) bool {
	return compareKeys(target.keys, constraint.keys)
}

func (constraint *Constraint) Equal(other *Constraint) (bool, error) {
	if strings.Compare(string(constraint.kind), string(other.kind)) != 0 {
		// If constraints aren't of the same kind there's no points in
		// comparing them, it's also not possible actually
		return false, nil
	} else {
		switch constraint.kind {
		case ForeignKey:
			if compareKeys(constraint.foreignKeys, other.foreignKeys) {
				return true, nil
			}
			return false, nil
		case Unique:
		case Check:
		case PrimaryKey:
			if compareKeys(constraint.keys, other.keys) {
				return true, nil
			}
			return false, nil
		}
	}
	return true, nil
}

func stringifyKeys(keys []*Column) string {
	var list []string
	list = make([]string, len(keys))
	for index, key := range keys {
		list[index] = key.name
	}
	return strings.Join(list, "\", \"")
}

func (constraint *Constraint) String() string {
	var table *Table = constraint.foreignTable
	switch constraint.kind {
	case PrimaryKey:
		return fmt.Sprintf("PRIMARY KEY (\"%s\")", stringifyKeys(constraint.keys))
	case ForeignKey:
		if table == nil {
			return ""
		}
		return fmt.Sprintf("FOREIGN KEY (\"%s\") REFERENCES \"%s\" (\"%s\")",
			stringifyKeys(constraint.keys),
			table.name,
			stringifyKeys(constraint.foreignKeys),
		)
	case Unique:
		return fmt.Sprintf("UNIQUE (\"%s\")", stringifyKeys(constraint.keys))
	}
	return ""
}

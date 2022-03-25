package main

import (
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

const DsnBase string = "host=%s port=%d user=%s dbname=%s sslmode=disable password=%s"

func main() {
	var err error
	var source *Schema
	var target *Schema
	var sql string
	var file *os.File

	source, err = NewSchema("localhost", 5432, "postgres", "", os.Args[1])
	if err != nil {
		panic(err)
	}

	target, err = NewSchema("localhost", 5432, "postgres", "", os.Args[2])
	if err != nil {
		panic(err)
	}

	sql, err = source.Diff(target)
	if err != nil {
		panic(err)
	}

	file, err = os.Create("migrate.sql")
	if err != nil {
		panic(err)
	}

	_, _ = file.WriteString(fmt.Sprintf("SET client_min_messages TO WARNING;\nBEGIN;\n%sROLLBACK;\n\n", sql))
}

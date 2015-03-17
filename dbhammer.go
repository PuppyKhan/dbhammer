// dbhammer.go

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/PuppyKhan/mysql"
)

/*
Test server environment:
export MYSQL_TEST_USER=root
export MYSQL_TEST_PASS=mysql
export MYSQL_TEST_DBNAME=gotest
export MYSQL_TEST_PROT=tcp
export MYSQL_TEST_ADDR=localhost
export MYSQL_TEST_CONCURRENT=1
export MYSQL_TEST_PORT=3306
*/

func main() {
	Names := []string{
		`Bob`,
		`Cathy`,
	}
	TraceLog := log.New(os.Stdout, "DB Hammer: ", log.Ldate|log.Ltime|log.Lshortfile)
	TraceLog.Println("Initializing")

	// shamelessly borrowed from https://github.com/go-sql-driver/mysql/blob/master/driver_test.go
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	MYSQL_TEST_USER := env("MYSQL_TEST_USER", "root")
	MYSQL_TEST_PASS := env("MYSQL_TEST_PASS", "mysql")
	MYSQL_TEST_PROT := env("MYSQL_TEST_PROT", "tcp")
	MYSQL_TEST_ADDR := env("MYSQL_TEST_ADDR", "localhost")
	MYSQL_TEST_PORT := env("MYSQL_TEST_PORT", "3306") // default
	MYSQL_TEST_DBNAME := env("MYSQL_TEST_DBNAME", "gotest")
	// MYSQL_TEST_CONCURRENT:=env("MYSQL_TEST_CONCURRENT")

	dsn := fmt.Sprintf("%s:%s@%s(%s:%s)/%s",
		MYSQL_TEST_USER, MYSQL_TEST_PASS, MYSQL_TEST_PROT,
		MYSQL_TEST_ADDR, MYSQL_TEST_PORT, MYSQL_TEST_DBNAME)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}

	db.SetMaxIdleConns(256)
	TraceLog.Println("Pinging ... ")

	err = db.Ping()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	stmt, err := db.Prepare("CREATE TABLE people (name VARCHAR(50) PRIMARY KEY);")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	_, err = stmt.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	stmt, err = db.Prepare("INSERT INTO people (name) VALUES (?);")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	for _, name := range Names {
		_, err = stmt.Exec(name)
		if err != nil {
			TraceLog.Fatal(err.Error())
		}
	}

	err = db.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Test done")
}

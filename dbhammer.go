// dbhammer.go

package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"

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

var (
	NumTries int = 100
	TraceLog *log.Logger
	Tags     []string = []string{"tag #1", "tag #2", "tag #3"}
)

func InsertRow(stmt *sql.Stmt, i int, wg *sync.WaitGroup) {
	var err error
	// just want a simple random string here, this onion has me crying...
	someText := fmt.Sprintf("%x", md5.Sum([]byte(strconv.Itoa(rand.Int()))))
	someTag := Tags[i%len(Tags)]
	TraceLog.Printf("Inserting %s, %s\n", someText, someTag)
	_, err = stmt.Exec(someText, someTag)
	if err != nil {
		TraceLog.Println(err.Error())
	}
	wg.Done()
}

func main() {
	var err error
	if len(os.Args) > 1 {
		NumTries, err = strconv.Atoi(os.Args[1])
		if err != nil {
			panic(err.Error())
		}
	}
	rand.Seed(int64(NumTries))
	TraceLog = log.New(os.Stdout, "DB Hammer: ", log.Ldate|log.Ltime|log.Lshortfile)
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

	// db.SetMaxIdleConns(256)
	TraceLog.Println("Pinging ... ")
	err = db.Ping()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Initializing tables")
	_, err = db.Exec("DROP TABLE IF EXISTS people;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = db.Exec("DROP TABLE IF EXISTS tagging;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	stmt, err := db.Prepare("CREATE TABLE tagging (tag VARCHAR(50) PRIMARY KEY);")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	err = stmt.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	stmt, err = db.Prepare("CREATE TABLE people (name VARCHAR(50) PRIMARY KEY, tag VARCHAR(50) NOT NULL, FOREIGN KEY (tag) REFERENCES tagging(tag));")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	err = stmt.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Populating table")
	var wg sync.WaitGroup
	stmt, err = db.Prepare("INSERT INTO people (name, tag) VALUES (?,?);")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	for i := 0; i < NumTries; i++ {
		wg.Add(1)
		go InsertRow(stmt, i, &wg)
	}
	wg.Wait()
	err = stmt.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Reading table by tag")
	stmt, err = db.Prepare("SELECT name, tag FROM people WHERE tag = ? LIMIT ?;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	var query_name sql.NullString
	var query_tag sql.NullString
	for i := 0; i < len(Tags); i++ {
		someTag := Tags[i]
		TraceLog.Printf("Reading %s...\n", Tags[i])
		rows, err := stmt.Query(someTag, NumTries)
		if err != nil {
			TraceLog.Println(err.Error())
		}
		defer func() {
			err = rows.Close()
			if err != nil {
				TraceLog.Fatal(err.Error())
			}
		}()

		for rows.Next() {
			err = rows.Scan(&query_name, &query_tag)
			if err != nil {
				TraceLog.Println(err.Error())
			}
			TraceLog.Printf("Returned row: %s, %s\n", query_name.String, query_tag.String)
		}
	}
	err = stmt.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Reading table count")
	stmt, err = db.Prepare("SELECT count(*) FROM people;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	var query_count int64
	err = stmt.QueryRow().Scan(&query_count)
	if err != nil {
		TraceLog.Println(err.Error())
	}
	TraceLog.Printf("Total row count: %d\n", query_count)
	err = stmt.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Dropping tables")
	stmt, err = db.Prepare("DROP TABLE people;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	err = stmt.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	stmt, err = db.Prepare("DROP TABLE tagging;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	err = stmt.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	err = db.Close()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Test done")
}

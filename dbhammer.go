// dbhammer.go

package main

import (
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"

	_ "github.com/PuppyKhan/mymysql/godrv"
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

intentionally mimicks github.com/go-sql-driver/mysql
*/

var (
	TraceLog       *log.Logger
	CreateTag      *sql.Stmt
	CreatePeople   *sql.Stmt
	InsertTag      *sql.Stmt
	SelectTagError *sql.Stmt
	InsertPeople   *sql.Stmt
	SelectPeople   *sql.Stmt
	SelectInSelect *sql.Stmt
	SelectCount    *sql.Stmt
	SPHello        *sql.Stmt
	// CreateSPHello  *sql.Stmt
)

// const storedProcHello = `
// DELIMITER //
// CREATE PROCEDURE hello_world() BEGIN SELECT 'Hello, World!'; END//
// DELIMITER ;
// `

func InsertRow(stmt *sql.Stmt, someTag string, wg *sync.WaitGroup) {
	var err error
	// just want a simple random string here, this onion has me crying...
	someText := fmt.Sprintf("%x", md5.Sum([]byte(strconv.Itoa(rand.Int()))))
	// someTag := Tags[i%len(Tags)]
	TraceLog.Printf("Inserting %s, %s\n", someText, someTag)
	_, err = stmt.Exec(someText, someTag)
	if err != nil {
		TraceLog.Println(err.Error())
	}
	wg.Done()
}

func PrepareCreates(db *sql.DB) {
	var err error
	CreateTag, err = db.Prepare("CREATE TABLE tagging (tag VARCHAR(50) PRIMARY KEY);")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	CreatePeople, err = db.Prepare("CREATE TABLE people (name VARCHAR(50) PRIMARY KEY, tag VARCHAR(50) NOT NULL, FOREIGN KEY (tag) REFERENCES tagging(tag));")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	// CreateSPHello, err = db.Prepare(storedProcHello)
	// if err != nil {
	// 	TraceLog.Fatal(err.Error())
	// }
}

func PrepareAll(db *sql.DB) {
	var err error
	InsertTag, err = db.Prepare("INSERT INTO tagging (tag) VALUES (?);")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	SelectTagError, err = db.Prepare("SELECT tag FROM tagging WHERE tag = ?;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	InsertPeople, err = db.Prepare("INSERT INTO people (name, tag) VALUES (?,?);")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	SelectPeople, err = db.Prepare("SELECT name, tag FROM people WHERE tag = ? LIMIT ?;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	SelectInSelect, err = db.Prepare("SELECT name, tag FROM people WHERE tag IN (SELECT tag FROM tagging) LIMIT 0,?;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	SelectCount, err = db.Prepare("SELECT count(*) FROM people;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	SPHello, err = db.Prepare("CALL hello_world;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

}

func CloseAll(db *sql.DB) {
	var err error
	if err = CreateTag.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = CreatePeople.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = InsertTag.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = SelectTagError.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = InsertPeople.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = SelectPeople.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = SelectInSelect.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = SelectCount.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	// if err = CreateSPHello.Close(); err != nil {
	// 	TraceLog.Fatal(err.Error())
	// }
	if err = SPHello.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}
}

func main() {
	var err error
	// Tags := []string{"tag #1", "tag #2", "tag #3"}
	var Tags []string = []string{"tag #1", "tag #2", "tag #3"}
	var query_name sql.NullString
	var query_tag sql.NullString
	var dsn string
	var driverName string

	conns := flag.Int("conns", 256, "Set # open/idle connections")
	tries := flag.Int("tries", 100, "Set # rows to try")
	forceSqlError := flag.Bool("error", false, "Test an error in SQL statement")
	useSP := flag.Bool("sp", false, "Test running stored procedure")
	driver := flag.String("db", "mysql", "Select driver: mymysql or mysql")
	flag.Parse()

	rand.Seed(int64(*tries))
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

	if *driver == "mymysql" {
		driverName = "mymysql"
		dsn = fmt.Sprintf("%s:%s:%s*%s/%s/%s",
			MYSQL_TEST_PROT, MYSQL_TEST_ADDR, MYSQL_TEST_PORT,
			MYSQL_TEST_DBNAME, MYSQL_TEST_USER, MYSQL_TEST_PASS)
	} else {
		driverName = "mysql" // default driver
		dsn = fmt.Sprintf("%s:%s@%s(%s:%s)/%s",
			MYSQL_TEST_USER, MYSQL_TEST_PASS, MYSQL_TEST_PROT,
			MYSQL_TEST_ADDR, MYSQL_TEST_PORT, MYSQL_TEST_DBNAME)
	}

	TraceLog.Println("Using", driverName)

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		panic(err.Error())
	}
	db.SetMaxIdleConns(*conns)
	db.SetMaxOpenConns(*conns)

	TraceLog.Println("Pinging ... ")
	err = db.Ping()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Preparations")
	// _, err = db.Exec("DROP PROCEDURE IF EXISTS hello_world;")
	// if err != nil {
	// 	TraceLog.Fatal(err.Error())
	// }
	_, err = db.Exec("DROP TABLE IF EXISTS people;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = db.Exec("DROP TABLE IF EXISTS tagging;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	PrepareCreates(db)

	TraceLog.Println("Initializing tables")
	_, err = CreateTag.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = CreatePeople.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	// TraceLog.Println("Initializing stored procedure")
	// _, err = CreateSPHello.Exec()
	// if err != nil {
	// 	TraceLog.Fatal(err.Error())
	// }

	TraceLog.Println("Preparing queries")
	PrepareAll(db)

	TraceLog.Println("Populating tags")
	for i := 0; i < len(Tags); i++ {
		someTag := Tags[i]
		TraceLog.Printf("Inserting tag: \"%s\"\n", someTag)
		_, err = InsertTag.Exec(someTag)
		if err != nil {
			TraceLog.Println(err.Error())
		}
	}

	if *useSP {
		TraceLog.Println("Testing Stored Procedure")

		TraceLog.Println("Prepared Stored Procedure Query")
		rows, err := SPHello.Query()
		if err != nil {
			TraceLog.Println(err.Error())
		}
		for rows.Next() {
			err = rows.Scan(&query_name)
			if err != nil {
				TraceLog.Println(err.Error())
			}
			TraceLog.Printf("Returned row: %s\n", query_name.String)
		}
		if err = rows.Err(); err != nil {
			TraceLog.Fatal(err.Error())
		}
		if err = rows.Close(); err != nil {
			TraceLog.Fatal(err.Error())
		}

		TraceLog.Println("Prepared Stored Procedure QueryRow")
		err = SPHello.QueryRow("CALL hello_world;").Scan(&query_name)
		if err != nil {
			TraceLog.Println(err.Error())
		}
		TraceLog.Printf("Returned row: %s\n", query_name.String)

		TraceLog.Println("Not prepared Stored Procedure QueryRow")
		err = db.QueryRow("CALL hello_world;").Scan(&query_name)
		if err != nil {
			TraceLog.Println(err.Error())
		}
		TraceLog.Printf("Returned row: %s\n", query_name.String)
	}

	if *forceSqlError {
		TraceLog.Println("Force Query() error")

		rows, err := SelectTagError.Query(Tags[0], "error param")
		if err != nil {
			TraceLog.Println(err.Error())
		} else {
			for rows.Next() {
				err = rows.Scan(&query_tag)
				if err != nil {
					TraceLog.Println(err.Error())
				}
				TraceLog.Printf("Returned row: %s\n", query_tag.String)
			}
			if err = rows.Err(); err != nil {
				TraceLog.Printf(err.Error())
			}
			if err = rows.Close(); err != nil {
				TraceLog.Fatal(err.Error())
			}
		}

		TraceLog.Println("Force Scan() error")
		rows, err = SelectTagError.Query(Tags[0])
		if err != nil {
			TraceLog.Println(err.Error())
		}
		for rows.Next() {
			err = rows.Scan(&query_name, &query_tag)
			if err != nil {
				TraceLog.Println(err.Error())
			}
			// TraceLog.Printf("Returned row: %s, %s\n", query_name.String, query_tag.String)
		}
		if err = rows.Err(); err != nil {
			TraceLog.Println(err.Error())
		}
		if err = rows.Close(); err != nil {
			TraceLog.Fatal(err.Error())
		}

	}

	TraceLog.Println("Populating table")
	var wg sync.WaitGroup
	for i := 0; i < *tries; i++ {
		wg.Add(1)
		go InsertRow(InsertPeople, Tags[i%len(Tags)], &wg)
	}
	wg.Wait()

	TraceLog.Println("Reading table by tag")
	for i := 0; i < len(Tags); i++ {
		someTag := Tags[i]
		TraceLog.Printf("Reading %s...\n", Tags[i])
		rows, err := SelectPeople.Query(someTag, *tries)
		if err != nil {
			TraceLog.Println(err.Error())
		}
		// defer func() {
		// 	if err = rows.Close(); err != nil {
		// 		TraceLog.Fatal(err.Error())
		// 	}
		// }()

		for rows.Next() {
			err = rows.Scan(&query_name, &query_tag)
			if err != nil {
				TraceLog.Println(err.Error())
			}
			TraceLog.Printf("Returned row: %s, %s\n", query_name.String, query_tag.String)
		}
		if err = rows.Err(); err != nil {
			TraceLog.Fatal(err.Error())
		}
		if err = rows.Close(); err != nil {
			TraceLog.Fatal(err.Error())
		}
	}

	TraceLog.Println("Inefficient query all")
	rows, err := SelectInSelect.Query(*tries)
	if err != nil {
		TraceLog.Println(err.Error())
	}
	for rows.Next() {
		err = rows.Scan(&query_name, &query_tag)
		if err != nil {
			TraceLog.Println(err.Error())
		}
		TraceLog.Printf("Returned row: %s, %s\n", query_name.String, query_tag.String)
	}
	if err = rows.Err(); err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = rows.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}

	TraceLog.Println("Reading table count")
	var query_count int64
	err = SelectCount.QueryRow().Scan(&query_count)
	if err != nil {
		TraceLog.Println(err.Error())
	}
	TraceLog.Printf("Total row count: %d\n", query_count)

	// do later prepares her on out
	TraceLog.Println("Dropping tables")

	// _, err = db.Exec("DROP PROCEDURE IF EXISTS hello_world;")
	// if err != nil {
	// 	TraceLog.Fatal(err.Error())
	// }

	// Received #1295 error from MySQL server: "This command is not supported in the prepared statement protocol yet"
	// stmt, err := db.Prepare("DROP PROCEDURE hello_world;")

	stmt, err := db.Prepare("DROP TABLE people;")
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		TraceLog.Fatal(err.Error())
	}
	if err = stmt.Close(); err != nil {
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
	if err = stmt.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}

	if err = db.Close(); err != nil {
		TraceLog.Fatal(err.Error())
	}

	CloseAll(db)
	TraceLog.Println("Test done")
}

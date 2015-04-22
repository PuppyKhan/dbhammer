# dbhammer

A simple MySQL Go driver test utility.

By Luigi Kapaj `<puppy at viahistoria.com>`

This requires a mysql server setup to run test scripts against, with location and credential set as environment variables, and (if used) a preset stored procedure.

## Test server environment variables
Location and credentials for the MySQL server are accessed through the following environment variables. Adjust as necessary for your local system.

```
export MYSQL_TEST_USER=root
export MYSQL_TEST_PASS=mysql
export MYSQL_TEST_DBNAME=gotest
export MYSQL_TEST_PROT=tcp
export MYSQL_TEST_ADDR=localhost
export MYSQL_TEST_PORT=3306
```

## Stored Procedure test
The test db requires this to be manual setup in order to use the -sp flag.

```
DELIMITER //
CREATE PROCEDURE hello_world() BEGIN SELECT 'Hello, World!'; END//
DELIMITER ;
```

## Imports
This utility makes use of all the following imports to build. The specific MySQL driver gets chosen at runtime.

```
go get -u github.com/PuppyKhan/mymysql/mysql
go get -u github.com/PuppyKhan/mymysql/native
go get -u github.com/PuppyKhan/mymysql/godrv
go get -u github.com/PuppyKhan/mysql
```

## Parameters and Defaults
```
$ dbhammer -help
Usage of dbhammer:
  -conns=256: Set # open/idle connections
  -db="mysql": Select driver: mymysql or mysql
  -error=false: Test an error in SQL statement
  -sp=false: Test running stored procedure
  -tries=100: Set # rows to try
```

## Sample running command
`dbhammer -db=mysql -conns=1 -sp`

This uses the "github.com/PuppyKhan/mysql" driver, only allows 1 open or idle connection, and tests stored procedures.

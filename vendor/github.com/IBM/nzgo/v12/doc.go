/*
Package nzgo is a pure Go language driver for the database/sql package to work with IBM PDA (aka Netezza)

In most cases clients will use the database/sql package instead of
using this package directly. For example:

	import (
		"database/sql"

		_ "github.com/IBM/nzgo/v12"
	)

	func main() {
		connStr := "user=nz dbname=db1 sslmode=verify-full "
		db, err := sql.Open("nzgo", connStr)
		if err != nil {
			log.Fatal(err)
		}

		age := 21
		rows, err := db.Query("SELECT name FROM users WHERE age = ?", age)
		…
	}

Logging

nzgo defines a simple logger interface. Set logLevel to control logging verbosity and logPath to specify log file path.
By default logging will be enabled with logLevel=Info and current directory as logPath.

You can configure logLevel and logPath (i.e. log file directory) as per your requirement.

There is one more configuration parameter with logger "additionalLogFile". This parameter can be used to set additional logger file.
additionalLogFile can be used to enable writing logs to stdout, this can be achieved by simply setting
"additionalLogFile=stdout"

Valid values for 'logLevel' are : "OFF" , "DEBUG", "INFO" and "FATAL".
logLevel=OFF can be used to turn off logging. It will turn of both internal and additionalLogFile logs.

These logger configuration parameters should be mentinoed in connection string.

SecurityLevel

The level of security (SSL/TLS) that the driver uses for the connection to the data store.

onlyUnSecured: The driver does not use SSL.
preferredUnSecured: If the server provides a choice, the driver does not use SSL.
preferredSecured: If the server provides a choice, the driver uses SSL.
onlySecured: The driver does not connect unless an SSL connection is available.

Similarly, Netezza server has above securityLevel.

Cases which would fail:
Client tries to connect with 'Only secured' or 'Preferred secured' mode while server is 'Only Unsecured' mode.
Client tries to connect with 'Only secured' or 'Preferred secured' mode while server is 'Preferred Unsecured' mode.
Client tries to connect with 'Only Unsecured' or 'Preferred Unsecured' mode while server is 'Only Secured' mode.
Client tries to connect with 'Only Unsecured' or 'Preferred Unsecured' mode while server is 'Preferred Secured' mode.

Below are the securityLevel you can pass in connection string :

	 0: Preferred Unsecured session
	 1: Only Unsecured session
	 2: Preferred Secured session
	 3: Only Secured session


Connection String

Use Open to create a database handle with connection parameters:
	db, err := sql.Open("nzgo", "<connection string>")

The Go Netezza Driver supports the following connection syntaxes (or data source name formats):

 "host=localhost user=admin dbname=db1 port=5480 password=password sslmode=require sslrootcert=C:/Users/root31.crt securityLevel=3 logLevel=Info logPath=./ additionalLogFile=stdout"

In this case, application is running from NPS server itself so using 'localhost'.
Golang driver should connect on port 5480(postgres port). The user is admin,
password is password, database is db1, sslmode is require, and the location of the root
certificate file is C:/Users/root31.crt with securityLevel as 'Only Secured session'


Connection Parameters

When establishing a connection using nzgo you are expected to
supply a connection string containing zero or more parameters.
Below are subset of the connection parameters supported by nzgo.

The following special connection parameters are supported:

	* dbname - The name of the database to connect to
	* user - The user to sign in as
	* password - The user's password
	* host - The host to connect to. Values that start with / are for unix
	  domain sockets. (default is localhost)
	* port - The port to bind to. (default is 5480)
	* sslmode - Whether or not to use SSL (default is require)
	* sslcert - Cert file location. The file must contain PEM encoded data.
	* sslkey - Key file location. The file must contain PEM encoded data.
	* sslrootcert - The location of the root certificate file. The file
	  must contain PEM encoded data.
	* logLevel - Log Level[Info/Debug/Fatal/Off]
	* logPath - Path to write log files
	* additionalLogPath - Additional log file can be mentioned here or stdout

Valid values for sslmode are:

	* disable - No SSL
	* require - Always SSL (skip verification)
	* verify-ca - Always SSL (verify that the certificate presented by the
	  server was signed by a trusted CA)

Use single quotes for values that contain whitespace:

    "user=nz password='with spaces'"

A backslash will escape the next character in values:

    "user=space\ man password='it\'s valid'"

Note that the connection parameter client_encoding (which sets the
text encoding for the connection) may be set but must be "UTF8",
matching with the same rules as Postgres. It is an error to provide
any other value.


Queries

database/sql does not dictate any specific format for parameter markers
in query strings, but nzgo uses the Netezza-specific parameter markers i.e. '?',
as shown below.

	rows, err := db.Query(`SELECT name FROM users WHERE favorite_fruit = ?
		OR age = ? `, "orange", 64)

First parameter marker in the query would be replaced by first arguement,
second parameter marker in the query would be replaced by second arguement
and so on.

nzgo supports the RowsAffected() method of the Result type in database/sql.

	var row int
	result, err := db.Exec(`INSERT INTO users(name, favorite_fruit, age)
		VALUES('beatrice', 'starfruit', 93) )
	if err == nil {
		row, _ := result.RowsAffected()
	}

For additional instructions on querying see the documentation for the database/sql package.
nzgo also supports transaction queries as specified in database/sql package https://github.com/golang/go/wiki/SQLInterface.

Transactions are started by calling Begin.

	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer tx.Rollback()

	_, err = tx.Exec("insert into foo(id) values (1)")
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}


Supported Data Types

This package returns the following types for values from the Netezza backend:

	- integer types byteint, smallint, integer, and bigint are returned as int8, int 16, int 32 and int64 respectively
	- floating-point types real and double precision are returned as float32 and float64 respectively
	- character types char, varchar, nchar and nvarchar are returned as string
	- temporal types date, time, timetz, timestamp, interval and timestamptz are
	  returned as string
	- numeric and geometry are returned as string
	- the boolean type is returned as bool


External table

You can unload data from an IBM Netezza database table on a Netezza host system to a remote client.
This unload does not remove rows from the database but instead stores the unloaded data in a flat file
(external table) that is suitable for loading back into a Netezza database.
Below query would create a file 'et1.txt' on remote system from Netezza table t2 with data delimeted by '|'.

	result, err := db.Exec("create external table et1 'C:\\et1.txt' using (remotesource 'golang' delim '|') as select * from t2;")
	if err != nil {
		fmt.Println("Error in creating external table", err)
	} else {
		fmt.Println("External Table created successfully")
	}


See https://www.ibm.com/support/knowledgecenter/en/SSULQD_7.2.1/com.ibm.nz.load.doc/t_load_unloading_data_remote_client_sys.html
for more information about external table

*/
package nzgo

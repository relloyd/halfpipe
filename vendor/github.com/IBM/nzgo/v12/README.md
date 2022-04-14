# nzgo - a pure Go language driver for IBM Performance Server for PostgreSQL
This project provides a native Go language database driver for IBM Performance Server for PostgreSQL.

[![Go Reference](https://pkg.go.dev/badge/github.com/IBM/nzgo/v12.svg)](https://pkg.go.dev/github.com/IBM/nzgo/v12)

## Install

`go get github.com/IBM/nzgo/v12`

## Docs

For detailed documentation and basic usage examples, please see the package
documentation at <https://godoc.org/github.com/IBM/nzgo/v12>.

## Tests

`go test` is used for testing.  See [TESTS.md](TESTS.md) for more details.

## Features

* TLSv1.2 crypto support
* LDAP support
* Transaction support: begin, rollback, commit
* Full support for all IBM PDA data types
* Full DDL, DML query syntax support for IBM PDA
* Full external table support (load and unload)
* Configurable logging feature
* Prepared statement support
* Support JSON, JSONB and JSONPATH datatypes

## Thank you (alphabetical)

Some of these contributions are from the original library `lib/pq` whose
code still exists in here. Below are the contributors for IBM PDA specific
code.

* Abhiskek Jog (abhishekjog)
* Sandeep Powar (sandippawar1412)
* Shabbir Mohammad (shabbir10july)

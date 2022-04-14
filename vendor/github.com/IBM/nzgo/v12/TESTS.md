# Tests

## Running Tests

`go test` is used for testing. A running IPS server is required, with the ability to log in. The
database to connect to test with is "db2" on "localhost" but these can be overridden using environment
variables. 

## Benchmarks

A benchmark suite can be run as part of the tests:

	go test -bench .

## Example setup

Run tests:

```
go test -run ''
```


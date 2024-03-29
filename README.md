# Halfpipe

[Introduction](#introduction) | [Feature Demos](#feature-demos) | [Usage](#usage) | [Deployment](#deployment) | [Quick Start](#quick-start) | 
[Sample Commands](#sample-commands) | [Serverless Support](#support-for-serverless-with-aws-lambda---12-factor-mode) | 
[Features Matrix](#features) | [Roadmap](#roadmap) | [Running In Production](#production) | [Notes](#notes) | 
[Limitations](#limitations) | [Contact](#want-to-know-more-or-have-a-feature-request) 


## Introduction

Halfpipe is a single-binary, command-line utility for streaming data to and from:

1. Snowflake
2. Oracle
3. SQL Server
4. S3 Buckets
5. Netezza
6. ODBC connections (bring your own database drivers)
7. Postgres support is on the [roadmap](#roadmap)

It reduces the complexity of common data integration patterns to single commands that run the same on your workstations 
as they do in production. Among other things it supports:

* Extracting snapshots and deltas periodically
* Oracle Continuous Query Notifications to stream in real-time (Oracle limitations apply)
* HTTP service to start/stop/launch jobs
* Automatic conversion of table metadata DDL
* [Serverless](#support-for-serverless-with-aws-lambda---12-factor-mode), for  use in AWS Lambda


## Feature Demos

![](./resources/halfpipe-subcommands.gif)

* [Copy table snapshots](./demo-svg/cp-snapshot/README.md)
* [Copy table changes / deltas](./demo-svg/cp-deltas/README.md)
* [Rip an entire Oracle schema to Snowflake](./demo-svg/cp-full-schema/README.md) (contains sample commands)
* [Start a micro-service to keep a Snowflake table up-to-date](./demo-svg/service/README.md)
* [Synchronise table data from a source to target](./demo-svg/sync-batch/README.md)
* [Stream table changes in real-time](./demo-svg/sync-events/README.md)
* [Validate table data (compare data and produce a diff report)](./demo-svg/diff/README.md)
* [Save the config file for a pipe action to run later](./demo-svg/pipes/README.md)
* [Configure connections](./demo-svg/connections/README.md)
* [Run in AWS Lambda](#support-for-serverless-with-aws-lambda---12-factor-mode)

More [sample commands](#sample-commands) are shown below.

## Usage 

```
$ hp
  ___ ___        .__   _____        __________.__
 /   |   \_____  |  |_/ ____\       \______   \__|_____   ____
/    ~    \__  \ |  |\   __\  ______ |     ___/  \____ \_/ __ \
\    Y    // __ \|  |_|  |   /_____/ |    |   |  |  |_> >  ___/
 \___|_  /(____  /____/__|           |____|   |__|   __/ \___  >
       \/      \/                                |__|        \/

Halfpipe is a DataOps utility for streaming data. It's designed to be light-weight and easy to use.
Use command-line switches for pre-canned actions or write your own pipes in YAML or JSON to sync
data in near real-time. Start an HTTP server to expose functionality via a RESTful API.
Halfpipe is not yet cluster-aware but it scales out. Start multiple instances of this tool and
off you go. Happy munging! 😄

Usage:
  hp [command]

Available Commands:
  config      Configure connections and default flag values
  cp          Copy snapshots, deltas or metadata from source objects to target
  create      Generate helpful metadata
  diff        Compare table data and report differences between source and target
  pipe        Execute a transform described in a YAML or JSON file
  query       Run a SQL query against a configured connection
  serve       Start a web service and listen for pipe commands described in JSON
  sync        Sync objects from source to target using batch or event-driven modes
  version     Show version information for Halfpipe
  help        Help about any command

Flags:
  -h, --help   help for hp

Use "hp [command] --help" for more information about a command.
```

## Deployment

Halfpipe is a light-weight Golang binary compiled against Oracle Database Instant Client drivers. 

Choose *__one__* of these options to get going:

#### A) Quick Start - Docker Image With Oracle Drivers Included

Use the [Quick Start](#quick-start) instructions below to build a local Docker image that contains the Halfpipe CLI and the Oracle client drivers. This is the easy option that drops you into a command prompt ready to use the `hp` tool or the [`configure.sh`](demo-svg/configure/README.md) script.

#### B) Bring You Own Oracle Drivers

Download one of the Release binaries and add it to your target environment. Optionally, copy the plugins to `/usr/local/lib` depending on your connectivity requirements to get started. 
If you don't want Oracle or ODBC functionality you should be good to just use the core `hp` standalone binary. 
On the other hand, if you want Oracle functionality via the Oracle plugin, you'll need the Oracle Instant Client installed and on your PATH. 

If you get an error like `hp: error while loading shared libraries: libclntsh.so.19.1: cannot open shared object file: No such file or directory` 
ensure your ORACLE_HOME environment variable is set and the OCI library is accessible.  
Follow Oracle's Instant Client set-up instructions and check SQL*Plus works then you should be good to go. 
The ODBC plugin requires unixODBC libraries. 

To configure connections, see the command usage or run the [`configure.sh`](demo-svg/configure/README.md) script to run a basic guided setup.

#### C) Compile From Source

Building the core `hp` binary is simple as only relies on native Go libraries.

For Oracle and ODBC connectivity, you'll need to have the Oracle instantclient (including SDK with header files) and ODBC drivers locally, as well
as to export DY_LD_LIBRARY_PATH (macOS) or LD_LIBRARY_PATH (linux) so the C linker can find the right shared objects:

```bash
# Build the core `hp` binary...
make build

# Build the plugins for Oracle and ODBC connectivity...
# Ensure DY_LD_LIBRARY_PATH or LD_LIBRARY_PATH variable is set.
make build-so

# Install the core `hp` binary AND plugins...
# Ensure DY_LD_LIBRARY_PATH or LD_LIBRARY_PATH variable is set.
make install
```

## Quick Start

The following [steps](#steps) will walk through option A above to:-

* build a local Docker image containing Halfpipe and Oracle client drivers
* add connections for an Oracle database, Snowflake and S3
* create a Snowflake external stage compatible with Halfpipe
* set default flag values for the `hp` CLI

After this, you'll be ready to use the example commands shown [below](#sample-commands) or in the [tl;dr section](#feature-demos) above. 

Good luck and drop me an [email](#want-to-know-more-or-have-a-feature-request) if you run into any issues. Happy munging! 😄  

### Prerequisites

1. Make
2. Docker
3. An S3 bucket to be used as an external Snowflake stage
4. A valid profile entry in AWS CLI file `~/.aws/credentials` (by default this needs to be called `halfpipe` - notes on how to override it are below) that can read/write the S3 bucket above
5. Oracle database connection details
6. Snowflake database connection details (please see the [`configure.sh`](demo-svg/configure/README.md) documentation to learn more if you're not using Snowflake)  

### Steps

```bash
# Build and start the Halfpipe Docker image with default AWS_PROFILE=halfpipe...
make quickstart

# From inside the Docker container, run this script to create connections and set default flag values.
# Follow the prompts and you're good to go:
./configure.sh -c
```

where: 

* `configure.sh -c` requests user input and runs the basic set-up to create connections to Oracle, Snowflake and S3. 
  Here's an example [transcript](./demo-svg/configure/README.md). See the help output of [`configure.sh -h`](./demo-svg/configure/README.md#usage-of-configuresh) 
  to learn more about the `hp` commands required to create connections and set default flag values.  


## Sample Commands

Once you have connections setup, it's as simple as a single command to copy data snapshots or deltas between databases. 

Here are a few sample actions - the demos above cover them all in more detail.

Alternatively head over to https://halfpipe.sh/in-detail/#learn to see a set of short videos that explain the features. 

```bash
# copy a snapshot of all data from Oracle table DIM_TIME to Snowflake via S3...
hp cp snap oracle.dim_time snowflake.dim_time

# copy a snapshot of all data from Oracle table DIM_TIME to an S3 bucket connection called demo-data-lake...
hp cp snap oracle.dim_time demo-data-lake

# copy data files from S3 bucket connection demo-data-lake into Snowflake 
# by filtering source objects using the regular expression myregexp...
hp cp snap demo-data-lake.myregexp snowflake.dim_time

# do the same as above, but only for changes that have appeared in S3 since the last time we looked
# instead of a regexp to filter source objects, this accepts an object name prefix only
# the object name format is fixed; see the command help for details of the format
# (SK_DATE is both the primary key and column that drives changes)
hp cp delta demo-data-lake.dim_time snowlfake.dim_time -p sk_date -d sk_date

# copy changes to Snowflake found in Oracle table DIM_TIME since the last time we looked
# repeat every hour...
# (SK_DATE is both the primary key and column that drives changes)
hp cp delta oracle.dim_time snowflake.dim_time -p sk_date -d sk_date -i 3600

# above we used a target connection called demo-data-lake for a S3 bucket - here's how to add it...
# more example of adding connections are in the Setup section below
hp config connections add s3 -c demo-data-lake -d s3://test.s3.reeslloyd.com

# copy a snapshot of all data from database OracleA, table DIM_TIME, to another Oracle database...
hp cp snap oracle.dim_time my-ora-connection.my_dim_time

# create a Snowflake STAGE called MYSTAGE to load data from S3 
# use -h for help or append -e to execute DDL...
hp create stage snowflake -s MYSTAGE

# synchronise all rows in source to target (make target data the same as source)...
hp sync batch oracle.dim_time snowflake.dim_time

# stream data from DIM_TIME to Snowflake in real-time... 
hp sync events oracle.dim_time snowflake.dim_time

# run a web service and listen for pipe actions in JSON/YAML...
# see the demos animations above for examples
hp serve 

# configure default flag values to save time having to supply them on the CLI...
hp config defaults -h

# configure source/target database connections and S3 bucket details...
hp config connections -h

# explore the demos above to see how you can add other connection types...
# or perform more simple actions to move data quickly.
```


## Support for Serverless with AWS Lambda - 12 Factor Mode

Halfpipe is small enough to run in AWS Lambda. 

To get started, please find a guide to creating a Lambda with sample environment variables on [my blog](https://halfpipe.sh/blog/07-running-halfpipe-in-aws-lambda/). 

In summary, the post describes how to create a Lambda with environment variables that mimic the existing CLI arguments and flags described above.

You can use the main Halfpipe binary `hp` (zipped ~7 MB) on its own when connecting between Snowflake, SQL Server and S3, 
but if you require Oracle or ODBC connectivity, you'll need to publish a Lambda layer with the Halfpipe plugins 
(see release binaries) and database client drivers.

### Usage

```bash
$ hp 12f

Halfpipe can be controlled by environment variables and is a good fit to run
in serverless environments where the binary size is compatible.

To enable Twelve-Factor mode, set environment variable HP_12FACTOR_MODE=1.
To supply flags documented by the regular command-line usage, set an
equivalent environment variable using the following convention:

<HP>_<flag long-name in upper case>

For example, this will copy a snapshot of data from sqlserver table
dbo.test_data_types to S3:

export HP_12FACTOR_MODE=1
export HP_LOG_LEVEL=debug
export HP_COMMAND=cp
export HP_SUBCOMMAND=snap
export HP_SOURCE_DSN='sqlserver://user:password@localhost:1433/database'
export HP_SOURCE_TYPE=sqlserver
export HP_TARGET_DSN=s3://test.halfpipe.sh/halfpipe
export HP_TARGET_TYPE=s3
export HP_TARGET_S3_REGION=eu-west-2
export HP_SOURCE_OBJECT=dbo.test_data_types
export HP_TARGET_OBJECT=test_data_types

Then execute the CLI tool without any arguments or flags to kick off the pipeline.
```


## Features

The CLI arguments for Halfpipe essentially use this format, where a logical connection is
required for each `source` and `target`:

`hp <command> <subcommand> <source>.<object> <target>.<object> [flags]`

The supported types of `source` and `target` connection are as follows, 
where `Y` shows current functionality and `r` shows a feature on the roadmap:

| ⬇️ `source` ➡️ `target` | Oracle | SQL Server | Postgres | S3  | Snowflake
| ---                      | ---    | ---        | ---      | --- | --- 
| Oracle                   | Y      | r          | r        | Y   | Y 
| SQL Server               | r      | r          | r        | Y   | Y
| Postgres                 | r      | r          | r        | r   | r
| S3                       | -      | -          | -        | -   | Y


## Roadmap

Some items on the roadmap include:

* Support for Snowflake in Azure and GCP 
* Oracle Change Data Capture (CDC)
* More database connectors (Postgres, Teradata, ...)
* AWS Marketplace
* Kafka and Kinesis streaming input/output
* User interface to manage the `hp serve` micro-service

Please [get in touch](#want-to-know-more-or-have-a-feature-request) if any of these stand out as being important to you.

## Production

To help fund and support this project, I've published an AWS machine image, with hourly billing and free trial available for you to get started.
Please take a look on the marketplace [here](https://aws.amazon.com/marketplace/pp/B08PL1STFS?qid=1610988617064&sr=0-1&ref_=srh_res_product_title).
There's no minimum commitment so it's easy to take it for a spin.
Let me know your thoughts any time as I really appreciate your feedback!

## Notes

The following files are AES-256 encrypted and base64 encoded at rest:

* Default flag values are picked up from file `~/.halfpipe/config.yml`
* Database connections are stored in file `~/.halfpipe/connections.yaml` 

Use the `config` CLI command to configure them.

## Limitations

* `hp sync events` does not work inside the Docker container fired up by `start-halfpipe.sh`... 
The issue is that Oracle expects to be able to notify the HalfPipe (`hp`) process using an 
open port. While the process appears to work and performs an initial table sync, 
it silently never receives further notifications events. 
Work around this when running the container on Linux by using host networking.
Alternatively, take the `hp` binary out of the container and use matching Oracle drivers directly
on a Linux host. 
* `hp sync events` will stream DML changes from source to target, but where >=100 rows
are committed per source transaction, it generates a full table re-sync as per the `hp sync batch`
command. This requires Oracle priv `GRANT CHANGE NOTIFICATION TO <user>` to work.
* `hp cp meta` doesn't produce ALTER TABLE statements yet. Drop the target table and recreate to 
work around this.


## Want to know more or have a feature request?

I'd welcome your feedback. 

Visit my website over at [halfpipe.sh](https://halfpipe.sh)

Get in touch by raising an issue above or via my website contact page. 

You can also email me directly at `richard at halfpipe dot sh`

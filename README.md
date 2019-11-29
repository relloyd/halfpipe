## Halfpipe

Halfpipe is a command-line utility for streaming 
data to and from the following RDBMS types:

1. Oracle
1. Snowflake
1. S3 (okay it's not an RDBMS, but it can be a target below)
1. (Postgres support pending - let me know if this would be useful)

Among other things it supports:

* Extracting snapshots periodically
* Extracting deltas periodically 
* Oracle Continuous Query Notifications to stream in real-time
* HTTP service to start/stop/launch jobs
* Automatic conversion of table metadata DDL


## tl;dr Demo Animations

* [Copy table snapshots](./demo-svg/cp-snapshot/README.md)
* [Copy table changes / deltas](./demo-svg/cp-deltas/README.md)
* [Rip an entire Oracle schema to Snowflake](./demo-svg/cp-full-schema/README.md)
* [Start a micro-service to keep a Snowflake table up-to-date](./demo-svg/service/README.md)
* [Synchronise table data from a source to target](./demo-svg/sync-batch/README.md)
* [Stream table changes in real-time](./demo-svg/sync-events/README.md)
* [Save the config file for a pipe action to run later](./demo-svg/pipes/README.md)
* [Configure connections to Oracle, Snowflake or S3](./demo-svg/connections/README.md)


## tl;dr Sample Commands

It's as simple as a single command to copy data snapshots or deltas between databases. 

Here are a few sample actions - the demos above cover them all in more detail. 

```bash
# copy a snapshot of all data from Oracle table DIM_TIME to Snowflake via S3...
hp cp snap oracleA.dim_time snowflake.dim_time

# copy a snapshot of all data from Oracle table DIM_TIME to a S3 bucket connection...
hp cp snap oracleA.dim_time demo-data-lake

# copy changes to Snowflake found in Oracle table DIM_TIME since the last time we looked, repeat every hour...
# (SK_DATE is both the primary key and column that drives changes)
hp cp delta oracleA.dwh.dim_time snowflake.dim_time -p sk_date -d sk_date -i 3600

# above we used a target connection for a S3 bucket called demo-data-lake - here's how to add it...
hp config connections add s3 -c demo-data-lake -d s3://test.s3.reeslloyd.com

# explore the demos above to see how you can add other connection types...
# or perform more simple actions to move data quickly.
```


## Usage


```
$ hp
  ___ ___        .__   _____        __________.__
 /   |   \_____  |  |_/ ____\       \______   \__|_____   ____
/    ~    \__  \ |  |\   __\  ______ |     ___/  \____ \_/ __ \
\    Y    // __ \|  |_|  |   /_____/ |    |   |  |  |_> >  ___/
 \___|_  /(____  /____/__|           |____|   |__|   __/ \___  >
       \/      \/                                |__|        \/

Half-Pipe is a DataOps utility for streaming data. It's designed to be light-weight and easy to use.
Use command-line switches for pre-canned actions or write your own pipes in YAML or JSON to sync
data in near real-time. Start an HTTP server to expose functionality via a RESTful API.
Half-Pipe is not yet cluster-aware but it scales out. Start multiple instances of this tool and
off you go. Happy munging! 😄

Usage:
  hp [command]

Available Commands:
  config      Configure connections and default flag values
  cp          Copy snapshots, deltas or metadata from source objects to target
  create      Generate helpful metadata
  pipe        Execute a transform described in a YAML or JSON file
  query       Run a SQL query against a configured connection
  serve       Start a web service and listen for pipe commands described in JSON
  sync        Sync objects from source to target using batch or event-driven modes
  help        Help about any command

Flags:
  -h, --help   help for hp

Use "hp [command] --help" for more information about a command.
```


## Want to know more or have a feature request?

I'd welcome your feedback. 

Get in touch by raising an issue above or email me directly at `relloyd@gmail.com`
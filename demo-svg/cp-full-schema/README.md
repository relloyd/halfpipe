#### Copy A Full Schema From Oracle To Snowflake
![Copy Full Schema From Oracle To Snowflake](./hp-oracle-full-schema-cp-snap.svg)

## Commands To Rip An Oracle Schema To Snowflake

Here are the sample commands from the demo animation above.

### Pre-requisites

__1) Configure default values__ for the following flags. 
This allows the commands further below to work in their simplest form:

```bash
$ hp config defaults -k s3-bucket -v <bucket>
$ hp config defaults -k s3-prefix -v <prefix>
$ hp config defaults -k s3-region -v <aws region e.g. eu-west-1>
$ hp config defaults -k s3-url -v s3://<bucket>/<prefix>  # ensure this matches the combined bucket and prefix used above (apologies for the duplication, i'll fix this soon)
$ hp config defaults -k stage -v <stage name>
```

__2) Create a Snowflake STAGE__ that is compatible with Halfpipe:

```bash
$ export AWS_ACCESS_KEY_ID=<key>
$ export AWS_SECRET_ACCESS_KEY=<secret>
$ hp create stage snowflake     # dumps the DDL
$ hp create stage snowflake -e  # executes the DDL above
``` 

__3) Create connections__ to Oracle & Snowflake:

  Please see the main README for set-up [instructions / commands](https://github.com/relloyd/halfpipe#setup).


---


### Steps

First, we'll create the target Snowflake tables by converting Oracle table data types to Snowflake.
Adjust the connection names `oracleA` and `snowflake` to match yours:

```bash
$ hp query oracleA select table_name from user_tables order by 1 | while read lin; do
        cmd="hp cp meta oracleA.$lin snowflake.$lin -e"
        echo $cmd
        eval $cmd
        done
```

Next, let's copy the tables straight to Snowflake via S3.
Adjust the connection names `oracleA` and `snowflake` to match yours:

```bash
$ hp query oracleA select table_name from user_tables order by 1 | while read lin; do
        cmd="hp cp snap oracleA.$lin snowflake.$lin"
        echo $cmd
        eval $cmd
        done
```

You can change the SQL used in the commands to meet your needs and 
don't forget to wrap the SQL in quotes to work around any shell expansion issues.
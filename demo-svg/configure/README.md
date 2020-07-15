# How To Configure Halfpipe in 2mins

Here's an animation of the Halfpipe CLI being started in a Docker image using script [`start-halfpipe.sh`](../../start-halfpipe.sh), which is included at the root of this repo.

The video shows how easy it is to set up Halfpipe with connections to Oracle, Snowflake and S3 - using script [`configure.sh`](#usage-of-configuresh) (added to the image).
 
The [original video](https://www.loom.com/share/a6a2743443f1480db50040e70aafe1f0) (with audio) is available on Loom.   

![](./halfpipe-setup.gif)

Here's the transcript from the animation above:

```
$ # How to get started with Halfpipe. Go from zero to migrated-data in 2 mins.
$
$ # First, let's clone the halfpipe Git repo...
$
$ git clone git@github.com:relloyd/halfpipe.git && cd halfpipe
$ Cloning into 'halfpipe'...
remote: Enumerating objects: 268, done.
remote: Counting objects: 100% (268/268), done.
remote: Compressing objects: 100% (201/201), done.
remote: Total 268 (delta 137), reused 181 (delta 60), pack-reused 0
Receiving objects: 100% (268/268), 323.87 KiB | 1.14 MiB/s, done.
Resolving deltas: 100% (137/137), done.

$ # ensure Docker is running and start Halfpipe as follows...
$ # this will build the Halfpipe Docker image with Oracle drivers and drop you into the container:
$
$ ./start-halfpipe.sh
$ Starting halfpipe:1 using AWS profile "halfpipe"...

  ___ ___        .__   _____        __________.__
 /   |   \_____  |  |_/ ____\       \______   \__|_____   ____
/    ~    \__  \ |  |\   __\  ______ |     ___/  \____ \_/ __ \
\    Y    // __ \|  |_|  |   /_____/ |    |   |  |  |_> >  ___/
 \___|_  /(____  /____/__|           |____|   |__|   __/ \___  >
       \/      \/                                |__|        \/

Halfpipe is a DataOps utility for streaming data. It's designed to be light-weight and easy to use.
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
  user        Log in or out and show Halfpipe session details
  version     Show version information for Halfpipe
  help        Help about any command

Flags:
  -h, --help   help for hp

Use "hp [command] --help" for more information about a command.

halfpipe:~ $ # the Halfpipe 'hp' command usage is above for info.
halfpipe:~ $ # now we can log in...
halfpipe:~ $ # supply your auth key or use this one that has privs to run core features:
halfpipe:~ $
halfpipe:~ $ hp user login aiHbKdA0lZIs3a2VCWySQyixfgTDxhRMlHNZ7bDGkes33.t8LTnzd38.anwJ1nc

Halfpipe session started:
  Current user:		tester1 at halfpipe dot sh
  Authorized until:	Thu, 31 Dec 2020 00:00:00 UTC
  Logout time:		Fri, 10 Jul 2020 11:48:12 UTC (23h59m59s)

halfpipe:~ $
halfpipe:~ $ # now let's set up database connections to Oracle, Snowflake and S3 etc.
halfpipe:~ $ # we'll run the 'configure.sh' script to simplify this...
halfpipe:~ $
halfpipe:~ $ ./configure.sh  # <<< this shows basic instructions by default; follow the suggested steps to get going...
halfpipe:~ $
Usage: ./configure.sh [-c | -e | -h]

  A script to configure Halfpipe with connections to Oracle, Snowflake
  and S3. It also sets default CLI flag values to simplify future
  'hp' commands.

  Prerequisites:

  1. A Snowflake instance and database connection details
  2. Oracle database connection details
  3. S3 bucket that can be used as part of a Snowflake external stage
  4. AWS IAM access keys for the bucket (read/write) configured in file
     ~/.aws/credentials, in profile/section: 'halfpipe'

  Suggested steps:

  1. Use -h to view detailed help instructions and understand the
     following steps:
  2. Use -c to configure values for environment variables that
     will be required by step-3:
  3. Use -e to execute Halfpipe commands that set up connections
     and default CLI flag values


halfpipe:~ $ # let's run it for real...
halfpipe:~ $
halfpipe:~ $ ./configure.sh -c  # <<< this will prompt for connection details...
halfpipe:~ $ Exporting variables. Hit enter to accept the current default
value in square brackets or supply a value...

Enter Oracle user (or export ORA_USER) [tester]:
Enter Oracle password (or export ORA_PASSWORD) [******ster]:
Enter Oracle host (or export ORA_HOST) [192.168.56.101]:
Enter Oracle port (or export ORA_PORT) [1521]:
Enter Oracle SID or service name (or export ORA_SERVICE) [orcl]:
Enter Snowflake user (or export SNOW_USER) [tester]:
Enter Snowflake password (or export SNOW_PASSWORD) [******jusi]:
Enter Snowflake database (or export SNOW_DATABASE) [demo_db]:
Enter Snowflake schema (or export SNOW_SCHEMA) [public]:
Enter Snowflake account (or export SNOW_ACCOUNT) [ea10989.eu-west-1]:
Enter Snowflake external stage name (or export SNOW_STAGE_NAME) [HALFPIPE_STAGE]:
Enter S3 region (or export BUCKET_REGION) [eu-west-2]:
Enter S3 bucket name (or export BUCKET_NAME) [test.halfpipe.sh]:
Enter S3 bucket prefix (or export BUCKET_PREFIX) [halfpipe]:
Enter AWS profile (or export AWS_PROFILE) [halfpipe]:
Enter Halfpipe log level (info|warn|error) (or export HP_LOG_LEVEL) [warn]:
Enter Halfpipe last modified field name (or export HP_LAST_MODIFIED_FIELD_NAME) [LAST_MODIFIED_DATE]:
Enter Halfpipe delta size (or export HP_DELTA_SIZE) [30]:

Configure Halfpipe connections and default flag values? [y]/n:
Execute:
  hp config connections add oracle -f -c oracle \
          -d "${ORA_USER}/${ORA_PASSWORD}@//${ORA_HOST}:${ORA_PORT}/${ORA_SERVICE}"
Continue? [y]/n:
Connection "oracle" added to "/home/dataops/.halfpipe/connections.yaml"
Execute:
  hp config connections add snowflake -f -c snowflake \
          -d "${SNOW_USER}:${SNOW_PASSWORD}@${SNOW_ACCOUNT}/${SNOW_DATABASE}?schema=${SNOW_SCHEMA}"
Continue? [y]/n:
Connection "snowflake" added to "/home/dataops/.halfpipe/connections.yaml"
Execute:
  hp config connections add s3 -f -c s3 \
          -d "s3://${BUCKET_NAME}/${BUCKET_PREFIX}"
Continue? [y]/n:
Connection "s3" added to "/home/dataops/.halfpipe/connections.yaml"
Execute:
  hp config defaults add -f -k s3-region -v "${BUCKET_REGION}"
Continue? [y]/n:
Key "s3-region" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  hp config defaults add -f -k s3-bucket -v "s3://${BUCKET_NAME}"
Continue? [y]/n:
Key "s3-bucket" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  hp config defaults add -f -k s3-prefix -v "${BUCKET_PREFIX}"
Continue? [y]/n:
Key "s3-prefix" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  hp config defaults add -f -k s3-url -v "s3://${BUCKET_NAME}/${BUCKET_PREFIX}"
Continue? [y]/n:
Key "s3-url" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  hp config defaults add -f -k stage -v "${SNOW_STAGE_NAME}"
Continue? [y]/n:
Key "stage" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  hp config defaults add -f -k log-level -v "${HP_LOG_LEVEL}"
Continue? [y]/n:
Key "log-level" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  hp config defaults add -f -k date-driver -v "${HP_LAST_MODIFIED_FIELD_NAME}"
Continue? [y]/n:
Key "date-driver" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  hp config defaults add -f -k delta-size -v "${HP_DELTA_SIZE}"
Continue? [y]/n:
Key "delta-size" added to "/home/dataops/.halfpipe/config.yaml"
Execute:
  AWS_ACCESS_KEY_ID="$(aws configure get ${AWS_PROFILE}.aws_access_key_id)" && export AWS_ACCESS_KEY_ID
Continue? [y]/n:
Execute:
  AWS_SECRET_ACCESS_KEY="$(aws configure get ${AWS_PROFILE}.aws_secret_access_key)" && export AWS_SECRET_ACCESS_KEY
Continue? [y]/n:
Preview DDL before creating Snowflake external stage (HALFPIPE_STAGE)? y/[n]:
Creating the external stage...
Execute:
  hp create stage snowflake -s ${SNOW_STAGE_NAME} -u "s3://${BUCKET_NAME}/${BUCKET_PREFIX}" -e
Continue? [y]/n:
Setup complete.

halfpipe:~ $
halfpipe:~ $ # so that's how to get set up with Halfpipe quickly 👍
halfpipe:~ $ # but what did that do?
halfpipe:~ $ # we can see that by using the following commands:
halfpipe:~ $
halfpipe:~ $ hp config connections list

snowflake:
  type = snowflake
  password = <encrypted>
  roleName =
  schemaName = public
  userName = tester
  warehouse =
  accountName = ea10989.eu-west-1
  databaseName = demo_db
oracle:
  type = oracle
  databaseName = orcl
  host = 192.168.56.101
  parameters = prefetch_rows=500
  password = <encrypted>
  port = 1521
  userName = tester
s3:
  type = s3
  name = test.halfpipe.sh
  prefix = halfpipe
  region = eu-west-2

halfpipe:~ $ hp config defaults list

log-level=warn
s3-bucket=s3://test.halfpipe.sh
s3-prefix=halfpipe
s3-region=eu-west-2
s3-url=s3://test.halfpipe.sh/halfpipe
stage=HALFPIPE_STAGE
date-driver=LAST_MODIFIED_DATE
delta-size=30

halfpipe:~ $ # now let's try out one of the simplest data migration features...
halfpipe:~ $ # here are some Oracle tables in our source database...
halfpipe:~ $
halfpipe:~ $ hp query oracle "select table_name from user_tables order by 1"
halfpipe:~ $ HP_DEMO_1_SNAPSHOT

halfpipe:~ $ # we'll migrate one to our Snowflake connection created above...
halfpipe:~ $
halfpipe:~ $ hp cp meta oracle.hp_demo_1_snapshot snowflake.hp_demo_1_snapshot -e
halfpipe:~ $
halfpipe:~ $ hp query snowflake show tables
halfpipe:~ $ 2020-07-09 11:49:45.195 +0000 UTC,HP_DEMO_1_SNAPSHOT,DEMO_DB,PUBLIC,TABLE,,,0,0,TESTER,1,OFF,OFF

halfpipe:~ $ # there it is, empty (zero rows)
halfpipe:~ $ # so we're ready to copy the data like this...
halfpipe:~ $
halfpipe:~ $ # hp cp snap oracle.hp_demo_1_snapshot snowflake.hp_demo_1_snapshot
halfpipe:~ $
halfpipe:~ $ # to see the 'cp snap' command and others in action, I have a series of short videos on my website, https://halfpipe.sh/in-detail/#learn
halfpipe:~ $ # now you're in a great place to play with all the other features of Halfpipe!
halfpipe:~ $ # thanks for watching and happy data munging 😄
```

# Usage Of `configure.sh`

```
➜  image git:(master) ✗ ./configure.sh -h
Usage: ./configure.sh [-c | -e | -h]

  A script to configure Halfpipe with connections to Oracle, Snowflake
  and S3. It also sets default CLI flag values to simplify future
  'hp' commands.

  Prerequisites:

  1. A Snowflake instance and database connection details
  2. Oracle database connection details
  3. S3 bucket that can be used as part of a Snowflake external stage
  4. AWS IAM access keys for the bucket (read/write) configured in file
     ~/.aws/credentials, in profile/section: 'default'

  Suggested steps:

  1. Use -h to view detailed help instructions and understand the
     following steps:
  2. Use -c to configure values for environment variables that
     will be required by step-3:
  3. Use -e to execute Halfpipe commands that set up connections
     and default CLI flag values

  where:

  -c  Requests user input for the following variables and shows
      their values if they're already set in the environment:"

        # ---------------------------------------------------------------------
        # Oracle credetials
        # ---------------------------------------------------------------------
        export ORA_USER=<user>
        export ORA_PASSWORD=<password>
        export ORA_HOST=<hostname>
        export ORA_PORT=<port>
        export ORA_SERVICE=<SID or service>

        # ---------------------------------------------------------------------
        # Snowflake credentials & external stage name
        # ---------------------------------------------------------------------
        export SNOW_USER=<user>
        export SNOW_PASSWORD=<password>
        export SNOW_DATABASE=<database>
        export SNOW_SCHEMA=<schema (optional)>
        export SNOW_ACCOUNT=<account>
        export SNOW_STAGE_NAME=<name of Snowflake external stage: default: 'HALFPIPE_STAGE'>

        # ---------------------------------------------------------------------
        # S3 bucket details to be used as a Snowflake external stage
        # ---------------------------------------------------------------------
        export BUCKET_REGION=<region e.g. eu-west-1>
        export BUCKET_NAME=<bucket name no trailing slash>
        export BUCKET_PREFIX=<bucket prefix no leading slash (default: 'halfpipe')>

        # ---------------------------------------------------------------------
        # AWS environment variables, used to fetch access keys for
        # creating a Snowflake external stage
        # ---------------------------------------------------------------------
        export AWS_PROFILE=<profile name from ~/.aws/credentials to find AWS access keys (default: 'default')>

        # ---------------------------------------------------------------------
        # Miscellaneous default CLI flag values
        # ---------------------------------------------------------------------
        export HP_LAST_MODIFIED_FIELD_NAME=<field name to drive incremental (cp delta) pipelines: default: 'LAST_MODIFIED_DATE'>
        export HP_DELTA_SIZE=<range of data to fetch in one chunk in incremental pipelins: default: 30>
        export HP_LOG_LEVEL=<info|warn|error (default: 'warn') where warn produces stats only>

  -c  Executes the following Halfpipe commands to perform initial setup.
      Each command will be printed and confirmation requested first.
      If the variables above are already set in your environment, you should
      be able to copy-paste this and execute it to achieve the same results.

        # ---------------------------------------------------------------------
        # Create connections with logical names:
        # 1. oracleA
        # 2. snowflake
        # 3. s3
        # ---------------------------------------------------------------------

        hp config connections add oracle -f -c oracleA \
          -d "${ORA_USER}/${ORA_PASSWORD}@//${ORA_HOST}:${ORA_PORT}/${ORA_SERVICE}"
        hp config connections add snowflake -f -c snowflake \
          -d "${SNOW_USER}:${SNOW_PASSWORD}@${SNOW_ACCOUNT}/${SNOW_DATABASE}?schema=${SNOW_SCHEMA}"
        hp config connections add s3 -f -c s3 \
          -d "s3://${BUCKET_NAME}/${BUCKET_PREFIX}"

        # ---------------------------------------------------------------------
        # Configure CLI default flag values to save having to enter them later.
        # The values are forced in with -f flag so beware existing values.
        # Ensure the value of s3-url matches the combined s3-bucket and
        # s3-prefix using the format: 's3://<bucket>/<prefix>'
        # The value of s3-prefix is optional.
        # Apologies for the duplication - i'll fix this soon.
        # ---------------------------------------------------------------------

        hp config defaults add -f -k s3-region -v "${BUCKET_REGION}"
        hp config defaults add -f -k s3-bucket -v "s3://${BUCKET_NAME}"
        hp config defaults add -f -k s3-prefix -v "${BUCKET_PREFIX}"
        hp config defaults add -f -k s3-url -v "s3://${BUCKET_NAME}/${BUCKET_PREFIX}"
        hp config defaults add -f -k stage -v "${SNOW_STAGE_NAME}"

        # ---------------------------------------------------------------------
        # Configure S3 IAM access keys.
        # Export AWS variables ready to setup a Snowflake external stage
        # They would normally be supplied as flags to the 'hp create' command
        # but this saves us from exposing secrets/values here.
        # This assumes you have a section in ~/.aws/credentials called
        # ''.
        # ---------------------------------------------------------------------

        AWS_ACCESS_KEY_ID="$(aws configure get ${AWS_PROFILE}.aws_access_key_id)" && export AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY="$(aws configure get ${AWS_PROFILE}.aws_secret_access_key)" && export AWS_SECRET_ACCESS_KEY

        # ---------------------------------------------------------------------
        # Create a Snowflake external stage called HALFPIPE_STAGE:
        # 1. Show Snowflake DDL
        # 2. Execute the DDL
        # ---------------------------------------------------------------------

        hp create stage snowflake -s ${SNOW_STAGE_NAME} -u "s3://${BUCKET_NAME}/${BUCKET_PREFIX}"
        hp create stage snowflake -s ${SNOW_STAGE_NAME} -u "s3://${BUCKET_NAME}/${BUCKET_PREFIX}" -e
```
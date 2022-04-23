#!/bin/bash

# Script to perform basic setup of Halfpipe with connections and default CLI flag values.
# Execute without any args for usage instructions, or see the usage functions below of course:

set -e  # exit upon failure

declare -a hp_create_connection_cmds
declare -a hp_configure_cli_cmds
declare -a hp_create_stage_cmds

script_dir=$(dirname "$0")
aws_default_profile=default
template_variables=$(cat "${script_dir}/.template-variables.env")

basic_usage_text="Usage: $0 [-c | -e | -h]

  A script to configure Halfpipe with connections to Oracle, Snowflake
  and S3. It also sets default CLI flag values to simplify future
  'hp' commands.

  Prerequisites:

  1. A Snowflake instance and database connection details
  2. Oracle database connection details
  3. S3 bucket that can be used as part of a Snowflake external stage
  4. AWS IAM access keys for the bucket (read/write) configured in file
     ~/.aws/credentials, in profile/section: '$aws_default_profile'

  Suggested steps:

  1. Use -h to view detailed help instructions and understand the
     following steps:
  2. Use -c to configure values for environment variables that
     will be required by step-3:
  3. Use -e to execute Halfpipe commands that set up connections
     and default CLI flag values
"

hp_create_connection_cmds=(
        [0]="hp config connections add oracle -f -c oracleA \\
          -d \"\${ORA_USER}/\${ORA_PASSWORD}@//\${ORA_HOST}:\${ORA_PORT}/\${ORA_SERVICE}\""
        [1]="hp config connections add snowflake -f -c snowflake \\
          -d \"\${SNOW_USER}:\${SNOW_PASSWORD}@\${SNOW_ACCOUNT}/\${SNOW_DATABASE}?schema=\${SNOW_SCHEMA}\""
        [2]="hp config connections add s3 -f -c s3 \\
          -d \"s3://\${BUCKET_NAME}/\${BUCKET_PREFIX}\""
)

hp_configure_cli_cmds=(
        [0]="hp config defaults add -f -k s3-region -v \"\${BUCKET_REGION}\""
        [1]="hp config defaults add -f -k s3-bucket -v \"s3://\${BUCKET_NAME}\""
        [2]="hp config defaults add -f -k s3-prefix -v \"\${BUCKET_PREFIX}\""
        [3]="hp config defaults add -f -k s3-url -v \"s3://\${BUCKET_NAME}/\${BUCKET_PREFIX}\""
        [4]="hp config defaults add -f -k stage -v \"\${SNOW_STAGE_NAME}\""
        [5]="hp config defaults add -f -k log-level -v \"\${HP_LOG_LEVEL}\""
        [6]="hp config defaults add -f -k date-driver -v \"\${HP_LAST_MODIFIED_FIELD_NAME}\""
        [7]="hp config defaults add -f -k delta-size -v \"\${HP_DELTA_SIZE}\""
)

hp_aws_cmds=(
        # set and export in separate commands to catch errors with 'set -e'.
        [0]="AWS_ACCESS_KEY_ID=\"\$(aws configure get \${AWS_PROFILE}.aws_access_key_id)\" && export AWS_ACCESS_KEY_ID"
        [1]="AWS_SECRET_ACCESS_KEY=\"\$(aws configure get \${AWS_PROFILE}.aws_secret_access_key)\" && export AWS_SECRET_ACCESS_KEY"
)

hp_create_stage_cmds=(
        [0]="hp create stage snowflake -s \${SNOW_STAGE_NAME} -u \"s3://\${BUCKET_NAME}/\${BUCKET_PREFIX}\""
        [1]="hp create stage snowflake -s \${SNOW_STAGE_NAME} -u \"s3://\${BUCKET_NAME}/\${BUCKET_PREFIX}\" -e"
)

usage_basic() {
  cat <<EOF >&2
${basic_usage_text}
EOF
  exit 1
}

usage_full() {
    cat <<EOF >&2
${basic_usage_text}
  where:

  -c  Requests user input for the following variables and shows
      their values if they're already set in the environment:"

${template_variables}

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

        ${hp_create_connection_cmds[0]}
        ${hp_create_connection_cmds[1]}
        ${hp_create_connection_cmds[2]}

        # ---------------------------------------------------------------------
        # Configure CLI default flag values to save having to enter them later.
        # The values are forced in with -f flag so beware existing values.
        # Ensure the value of s3-url matches the combined s3-bucket and
        # s3-prefix using the format: 's3://<bucket>/<prefix>'
        # The value of s3-prefix is optional.
        # Apologies for the duplication - i'll fix this soon.
        # ---------------------------------------------------------------------

        ${hp_configure_cli_cmds[0]}
        ${hp_configure_cli_cmds[1]}
        ${hp_configure_cli_cmds[2]}
        ${hp_configure_cli_cmds[3]}
        ${hp_configure_cli_cmds[4]}

        # ---------------------------------------------------------------------
        # Configure S3 IAM access keys.
        # Export AWS variables ready to setup a Snowflake external stage
        # They would normally be supplied as flags to the 'hp create' command
        # but this saves us from exposing secrets/values here.
        # This assumes you have a section in ~/.aws/credentials called
        # '${AWS_PROFILE}'.
        # ---------------------------------------------------------------------

        ${hp_aws_cmds[0]}
        ${hp_aws_cmds[1]}

        # ---------------------------------------------------------------------
        # Create a Snowflake external stage called HALFPIPE_STAGE:
        # 1. Show Snowflake DDL
        # 2. Execute the DDL
        # ---------------------------------------------------------------------

        ${hp_create_stage_cmds[0]}
        ${hp_create_stage_cmds[1]}

EOF
  exit 1
}

function exportVariable() {
  # Function to prompt for user input into a given variable by name.
  # If a value is set for the given variable then it is presented as the default value.
  # If the user hits return they accept the current default, else the new value is read into the variable.
  # $1 = variable name
  # $2 = prompt / text
  # $3 = set this to cause silent input for reading passwords (any non-zero value)
  # $4 = set this to supply a default value (optional)
  var=$1
  default="${!var}"
  prompt=$2
  secret=$3
  override=$4
  if [[ -z "$default" && -n "$override" ]]; then  # if there is no default but we have been given one to override...
    default="${override}"  # set the default explicitely.
    export "${var}"="${override}" # export, i.e. reset the input variable to the default value.
    # Don't print the default as it's going to be in square brackets:
    #   prompt=$(printf "%s %s" "${prompt}" "(default: '${default}')")  # show the default in the prompt.
  fi
  if [[ -n ${default} ]]; then # if there is a default value for the supplied variable...
    # print the prompt and request for input with the current default value in brackets.
    if [[ -n "${secret}" ]]; then # if we should keep the input value secret...
      # print the prompt with obfuscated default value...
      obfuscated="******${default:${#default}-4:${#default}}"
      printf "%s [%s]: " "$prompt" "$obfuscated"
    else  # else we can print the full default value...
      printf "%s [%s]: " "$prompt" "$default"
    fi
  else # else there is no current default...
    printf "%s: " "$prompt" # request user input.
  fi
  if [[ -n "${secret}" ]]; then # if we should read a password...
    read -rs # use silent; do not echo the input.
    echo ""  # simulate new line since silent mode doesn't print it.
  else # else we can echo the user input...
    read -r
  fi
  if [[ -n $REPLY ]]; then # if the user supplied a new value...
    export "${var}"="${REPLY}" # export, i.e. reset the input variable to the new value.
  fi
}

function executeCmd() {
  # Execute a single command supplied as $1, with user confirmation requested to continue.
  cmd=$1
  skip_confirmation=$2
  echo "Execute:"
  echo "  ${cmd}"
  if [[ -z "${skip_confirmation}" ]]; then
    printf "Continue? [y]/n: "
    read -r
  fi
  if [[ -z "${REPLY}" || "${REPLY}" == 'Y' || -n "${skip_confirmation}" ]]; then  # if the user wants to continue...
    eval "${cmd}"
  else
    echo Skipped.
  fi
}

function executeHalfpipeSetupCommands() {
  # Execute commands configured in arrays declared above and hardcoded below.
  printf "Configure Halfpipe connections and default flag values? [y]/n: "
  read -r
  if [[ -z "${REPLY}" || "${REPLY}" == 'y' ]]; then  # if the user wants to configure Halfpipe...
    for i in "${!hp_create_connection_cmds[@]}"; do
      executeCmd "${hp_create_connection_cmds[$i]}"
    done
    for i in "${!hp_configure_cli_cmds[@]}"; do
      if [[ "${BUCKET_PREFIX}" == "" ]]; then  # Hack to supply empty value for BUCKET_PREFIX until this script is moved into the hp CLI. Added to Trello 2020-06-30.
        export BUCKET_PREFIX=" "
      fi
      executeCmd "${hp_configure_cli_cmds[$i]}"
    done
    for i in "${!hp_aws_cmds[@]}"; do
      executeCmd "${hp_aws_cmds[$i]}"
    done
    # Check if user wants to preview DDL for CREATE STAGE command:
    printf "Preview DDL before creating Snowflake external stage (%s)? y/[n]: " "${SNOW_STAGE_NAME}"
    read -r
    if [[ "${REPLY}" == "y" ]]; then  # if we should show Snowflake CREATE STAGE DDL first...
      executeCmd "${hp_create_stage_cmds[0]}" "skip-confirmation"
    fi
    echo "Creating the external stage..."
    executeCmd "${hp_create_stage_cmds[1]}" # execute the DDL to create the Snowflake stage.
  fi
  echo "Setup complete."
}

###############################################################################
# MAIN
###############################################################################

while getopts ":ceh" o; do
    case "${o}" in
        c)
            action=configure;;
        e)
            action=execute;;
        h)
            usage_full;;
        *)
            usage_basic;;
    esac
done
shift $((OPTIND-1))

if [[ "${action}" == "configure" ]]; then
  # Set variables.
  echo "Exporting variables. Hit enter to accept the current default"
  echo "value in square brackets or supply a value..."
  echo ""
  exportVariable "ORA_USER" "Enter Oracle user (or export ORA_USER)"
  exportVariable "ORA_PASSWORD" "Enter Oracle password (or export ORA_PASSWORD)" "secret"
  exportVariable "ORA_HOST" "Enter Oracle host (or export ORA_HOST)"
  exportVariable "ORA_PORT" "Enter Oracle port (or export ORA_PORT)"
  exportVariable "ORA_SERVICE" "Enter Oracle SID or service name (or export ORA_SERVICE)"
  exportVariable "SNOW_USER" "Enter Snowflake user (or export SNOW_USER)"
  exportVariable "SNOW_PASSWORD" "Enter Snowflake password (or export SNOW_PASSWORD)" "secret"
  exportVariable "SNOW_DATABASE" "Enter Snowflake database (or export SNOW_DATABASE)"
  exportVariable "SNOW_SCHEMA" "Enter Snowflake schema (or export SNOW_SCHEMA)"
  exportVariable "SNOW_ACCOUNT" "Enter Snowflake account (or export SNOW_ACCOUNT)"
  exportVariable "SNOW_STAGE_NAME" "Enter Snowflake external stage name (or export SNOW_STAGE_NAME)" "" "HALFPIPE_STAGE"
  exportVariable "BUCKET_REGION" "Enter S3 region (or export BUCKET_REGION)"
  exportVariable "BUCKET_NAME" "Enter S3 bucket name (or export BUCKET_NAME)"
  exportVariable "BUCKET_PREFIX" "Enter S3 bucket prefix (or export BUCKET_PREFIX)" "" "halfpipe"
  exportVariable "AWS_PROFILE" "Enter AWS profile (or export AWS_PROFILE)" "" "$aws_default_profile"
  exportVariable "HP_LOG_LEVEL" "Enter Halfpipe log level (info|warn|error) (or export HP_LOG_LEVEL)" "" "warn"
  exportVariable "HP_LAST_MODIFIED_FIELD_NAME" "Enter Halfpipe last modified field name (or export HP_LAST_MODIFIED_FIELD_NAME)" "" "LAST_MODIFIED_DATE"
  exportVariable "HP_DELTA_SIZE" "Enter Halfpipe delta size (or export HP_DELTA_SIZE)" "" "30"
  # Configure Halfpipe.
  echo ""
  executeHalfpipeSetupCommands
elif [[ "${action}" == "execute" ]]; then
  executeHalfpipeSetupCommands
else
  usage_basic
fi

exit 0

#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( BQ_TABLE_PREFIX )

display_usage() {
    echo -e "Updates the schema of every NAF BigQuery sharded table for a given date range to the current schema.\n"
    echo -e "It will iterate over the dates between START_DATE and END_DATE (inclusive) and update the schema of each table using the table_prefix_YYYYMMDD format applying the new schema defined in assets/naf-schema.json file."
    echo -e "Additionally, it will set labels to the table indicating the component and version of the pipe_naf_reader package used.\n"
    echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
    echo -e "\tBQ_TABLE_PREFIX: The fully qualified table name prefix (project.dataset.table_prefix)."
    echo -e ""
}

display_error() {
    echo -e "\nError: $1\n"
}


if [[ $# -ne ${#ARGS[@]} ]]; then
    display_usage
    exit 1
fi

arg_values=("$@")
params=()
for index in ${!ARGS[*]}; do
  echo "${ARGS[$index]}=${arg_values[$index]}"
  declare "${ARGS[$index]}"="${arg_values[$index]}"
done


BQ_PATTERN="^[a-zA-Z0-9_\-]+[\.:][a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+$"
if [[ "${BQ_TABLE_PREFIX}" =~ ${BQ_PATTERN} ]]; then
  # if colon punctuation is not present replace only the first dot with colon punctuation.
  BQ_TABLE_PREFIX=$(if [[ ${BQ_TABLE_PREFIX} != *":"*  ]]; then echo ${BQ_TABLE_PREFIX/./:}; else echo ${BQ_TABLE_PREFIX}; fi)
else
  display_error "Error passing the BQ_TABLE_PREFIX = ${BQ_TABLE_PREFIX} it should match the following pattern (${BQ_PATTERN})."
  exit 1
fi


echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

# Get list of all shard tables matching the table name to upgrade schema and store them in an array variable
echo "Looking for tables with prefix '${BQ_TABLE_PREFIX}' that do not have version label '${LABELS_VERSION}'"

SHARD_TABLES=($(python $THIS_SCRIPT_DIR/tables_to_upgrade.py --table_prefix "$BQ_TABLE_PREFIX" --labels_version "$LABELS_VERSION"))
if [ ${#SHARD_TABLES[@]} -eq 0 ]; then
    echo "No tables found to upgrade schema."
    exit 0
fi
for BQ_TABLE in "${SHARD_TABLES[@]}"; do
    # set the table label component to the current package name and version
    bq update \
        --schema=$ASSETS/naf-schema.json \
        $(for label in "${LABELS[@]}"; do echo "--set_label=${label} "; done) \
        "$BQ_TABLE"
    if [ "$?" -ne 0 ]; then
        echo "  Failed to update schema for table: $BQ_TABLE"
        exit 1
    fi
done

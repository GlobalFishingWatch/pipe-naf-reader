#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets

PROCESS=$(basename $0 .sh)
ARGS=( BQ_TABLE_PREFIX \
  START_DATE \
  END_DATE )

display_usage() {
    echo -e "Updates the schema of every NAF BigQuery sharded table for a given date range to the current schema.\n"
    echo -e "It will iterate over the dates between START_DATE and END_DATE (inclusive) and update the schema of each table using the table_prefix_YYYYMMDD format applying the new schema defined in assets/naf-schema.json file."
    echo -e "Additionally, it will set labels to the table indicating the component and version of the pipe_naf_reader package used.\n"
    echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
    echo -e "\tBQ_TABLE_PREFIX: The fully qualified table name prefix (project.dataset.table_prefix)."
    echo -e "\tSTART_DATE: The start date in format YYYY-MM-DD."
    echo -e "\tEND_DATE: The end date in format YYYY-MM-DD."
    echo -e ""
}

display_error() {
    echo -e "\nError: $1\n"
}

calculate_next_date() {
    local current_date="$1"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        date -j -f "%Y-%m-%d" -v+1d "$current_date" +"%Y-%m-%d"
    else
        # Linux and other Unix-like systems
        date -I -d "$current_date + 1 day"
    fi
}

if [[ $# -ne ${#ARGS[@]} ]]
then
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

# get the current package version and store in a variable
PACKAGE_VERSION=$(
python3 -c "import importlib, importlib.util; m = importlib.import_module('importlib.metadata' if importlib.util.find_spec('importlib.metadata') else 'importlib_metadata'); print(m.version('pipe_naf_reader'))"
)
# make the package version compatible with bq label requirements
PACKAGE_VERSION=${PACKAGE_VERSION//[^a-zA-Z0-9_\-]/_}

# validate that start_date and end_date are in the correct format YYYY-MM-DD 
# and that start_date is less than or equal to end_date
date -d "$START_DATE" +"%Y-%m-%d" >/dev/null 2>&1
if [ "$?" -ne 0 ]; then
    display_error "START_DATE is not in the correct format YYYY-MM-DD."
    exit 1
fi
date -d "$END_DATE" +"%Y-%m-%d" >/dev/null 2>&1
if [ "$?" -ne 0 ]; then
    display_error "END_DATE is not in the correct format YYYY-MM-DD."
    exit 1
fi
if [ "$(date -d "$START_DATE" +%s)" -gt "$(date -d "$END_DATE" +%s)" ]; then
    display_error "START_DATE cannot be greater than END_DATE."
    exit 1
fi

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

CURRENT_DATE="$START_DATE"

# iterate over the dates from START_DATE to END_DATE (inclusive)
END_DATE_PLUS_ONE=$(calculate_next_date "$END_DATE")
while [ "$CURRENT_DATE" != "$END_DATE_PLUS_ONE" ]; do
    BQ_TABLE="${BQ_TABLE_PREFIX}_$(echo $CURRENT_DATE | tr -d '-')"
    echo "Updating schema for table: $BQ_TABLE with package version: $PACKAGE_VERSION"
    # set the table label component to the current package name and version
    bq update \
        --schema=$ASSETS/naf-schema.json \
        --set_label="component:pipe_naf_reader" \
        --set_label="version:$PACKAGE_VERSION" \
        "$BQ_TABLE"
    if [ "$?" -ne 0 ]; then
        echo "  Failed to update schema for table: $BQ_TABLE"
        exit 1
    fi

    # Increment date by one day
    CURRENT_DATE=$(calculate_next_date "$CURRENT_DATE")
done

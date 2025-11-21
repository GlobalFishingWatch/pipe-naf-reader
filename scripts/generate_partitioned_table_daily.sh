#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( BQ_INPUT \
  BQ_OUTPUT \
  DATE_RANGE )

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

display_usage() {
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
  echo -e "BQ_INPUT: BigQuery dataset and table where the input is already stored (Format expected <DATASET>.<TABLE>).\n"
  echo -e "BQ_OUTPUT: BigQuery dataset and table where will be stored the output (Format expected <DATASET>.<TABLE>).\n"
  echo -e "DATE_RANGE: Two dates separated by a comma (Format: YYYY-MM-DD,YYYY-MM-DD). Used to filter _TABLE_SUFFIX in the query.\n"
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

# Parse DATE_RANGE into START_DATE and END_DATE
IFS=',' read -r START_DATE END_DATE <<< "$DATE_RANGE"
if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "Error: DATE_RANGE must be two dates separated by a comma (YYYY-MM-DD,YYYY-MM-DD)"
  display_usage
  exit 1
fi

# Convert START_DATE and END_DATE to variables compatible with BigQuery
START_DATE=${START_DATE//-/}
END_DATE=${END_DATE//-/}

# BQ_INPUT_PATH=${BQ_INPUT}_${DS//-/}
# BQ_PATTERN="^[a-zA-Z0-9_\-]+[\.:][a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+$"
if [[ "${BQ_OUTPUT}" =~ ${BQ_PATTERN} ]]; then
  # if colon punctuation is not present replace only the first dot with colon punctuation.
  BQ_OUTPUT_COLON=$(if [[ ${BQ_OUTPUT} != *":"*  ]]; then echo ${BQ_OUTPUT/./:}; else echo ${BQ_OUTPUT}; fi)
else
  echo "Error passing the BQ_OUTPUT it should match the following pattern (${BQ_PATTERN})."
  exit 1
fi
################################################################################
################################################################################
# Executes query reading all shards of the input table
################################################################################
echo "Executes query reading all shards of the input table ${BQ_INPUT}"
QUERY=${ASSETS}/naf-process.sql.j2
SQL=$(jinja2 ${QUERY} -D source=${BQ_INPUT} -D start_date=${START_DATE} -D end_date=${END_DATE})
echo "Query to be executed:"
echo "${SQL}"
################################################################################
echo "${SQL}" | bq query \
  --max_rows=0 \
  --allow_large_results \
  --append_table \
  --nouse_legacy_sql \
  --destination_schema ${ASSETS}/naf-process-schema.json \
  --destination_table ${BQ_OUTPUT_COLON} \
  --time_partitioning_field timestamp \
  --clustering_fields shipname,callsign,external_id

if [ "$?" -ne 0 ]; then
  echo "  Unable to create table ${BQ_OUTPUT_COLON}"
  exit 1
fi
################################################################################
# Updates the description of the output table
################################################################################
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${BQ_INPUT}"
  "* Date Range: ${START_DATE} to ${END_DATE}"
  "* Last Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )
echo "Updating table description ${BQ_OUTPUT_COLON}"
bq update \
  --description "${TABLE_DESC}" \
  $(for label in "${LABELS[@]}"; do echo "--set_label=${label} "; done) \
  ${BQ_OUTPUT_COLON}
if [ "$?" -ne 0 ]; then
  echo "  Unable to update the description table ${BQ_OUTPUT_COLON}"
  exit 1
fi
echo "  ${BQ_OUTPUT_COLON} Done."

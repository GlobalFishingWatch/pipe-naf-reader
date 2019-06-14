#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh
source pipe-tools-utils

PROCESS=$(basename $0 .sh)
ARGS=( NAME  \
  BQ_INPUT \
  BQ_OUTPUT \
  DS )

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

display_usage() {
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
  echo -e "NAME: Name of the country that gives the NAF files.\n"
  echo -e "BQ_INPUT: BigQuery dataset and table where the input is already stored (Format expected <DATASET>.<TABLE>).\n"
  echo -e "BQ_OUTPUT: BigQuery dataset and table where will be stored the output (Format expected <DATASET>.<TABLE>).\n"
  echo -e "DS: The date expressed with the following format YYYY-MM-DD. To be used for request.\n"
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

YYYYMMDD=$(yyyymmdd ${DS})
BQ_INPUT_PATH=${BQ_INPUT}_${YYYYMMDD}
BQ_OUTPUT_PATH=${BQ_OUTPUT}_${YYYYMMDD}
################################################################################
# Executes query reading the input table
################################################################################
echo "Executes query reading the input table ${BQ_INPUT_PATH}"
QUERY=${ASSETS}/naf-process-${NAME}.sql.j2
SQL=$(jinja2 ${QUERY} -D source=${BQ_INPUT_PATH})
echo "${SQL}" | bq query \
    --max_rows=0 \
    --allow_large_results \
    --replace \
    --nouse_legacy_sql \
    --destination_schema ${ASSETS}/naf-process-${NAME}-schema.json \
    --destination_table ${BQ_OUTPUT_PATH} \
    --time_partitioning_field timestamp

if [ "$?" -ne 0 ]; then
  echo "  Unable to create table ${BQ_OUTPUT_PATH}"
  exit 1
fi
################################################################################
# Updates the description of the output table
################################################################################
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${BQ_INPUT_PATH}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )
echo "Updating table description ${BQ_OUTPUT_PATH}"
bq update --description "${TABLE_DESC}" ${BQ_OUTPUT_PATH}
if [ "$?" -ne 0 ]; then
  echo "  Unable to update the description table ${BQ_OUTPUT_PATH}"
  exit 1
fi
echo "  ${BQ_OUTPUT_PATH} Done."

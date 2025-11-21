#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
LIB=${THIS_SCRIPT_DIR}/../pipe_naf_reader
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( GCS_SOURCE \
  GCS_CSV_OUTPUT \
  BQ_OUTPUT \
  DS )

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

display_usage() {
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
  echo -e "GCS_SOURCE: Source of the GCS where the NAF files are stored (Format expected gs://<BUCKET>/<OBJECT>).\n"
  echo -e "GCS_CSV_OUTPUT: Folder where to store the CSVs output result from the NAF parser (Format expected gs://<BUCKET>/<OBJECT>).\n"
  echo -e "BQ_OUTPUT: BigQuery project, dataset and table where will be stored the output (Format expected <PROJECT>.<DATASET>.<TABLE>).\n"
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

GCS_DATE_SOURCE=${GCS_SOURCE}/${DS}
################################################################################
# Prepare GCS CSV path and check if processing can be skipped
################################################################################
GCS_CSV_FILE=${GCS_CSV_OUTPUT}/${DS}.csv

# If the CSV exists and is newer or equal than the most recent source file, skip the section.
SKIP_PROCESSING=0
if gsutil -q stat "${GCS_CSV_FILE}" 2>/dev/null; then
  echo "Found existing CSV at ${GCS_CSV_FILE}. Checking timestamps..."
  csv_ts=$(gsutil ls -l "${GCS_CSV_FILE}" 2>/dev/null | grep -E 'gs://' | awk '{print $2}')
  # Get the most recent timestamp among the files under the source path
  latest_src_ts=$(gsutil ls -l "${GCS_DATE_SOURCE}"/* 2>/dev/null | grep -E 'gs://' | awk '{print $2}' | sort -r | head -n1)

  if [ -z "${latest_src_ts}" ]; then
    echo "  No source files found in ${GCS_DATE_SOURCE}. Will skip processing."
    SKIP_PROCESSING=1
  else
    # ISO 8601 timestamps compare lexicographically
    if [[ "${csv_ts}" > "${latest_src_ts}" ]]; then
      echo "  Remote CSV (${csv_ts}) is newer than latest source file (${latest_src_ts}). Skipping processing."
      SKIP_PROCESSING=1
    else
      echo "  Remote CSV (${csv_ts}) is older than latest source file (${latest_src_ts}). Proceeding with processing."
    fi
  fi
else
  echo "No existing CSV at ${GCS_CSV_FILE}. Proceeding with processing."
fi

if [[ ${SKIP_PROCESSING} -ne 1 ]]; then

  ################################################################################
  # Download files locally
################################################################################
LOCAL_RAW_NAF_PATH=./data/raw_naf
echo "Downloading records from source ${GCS_DATE_SOURCE} to local disk ${LOCAL_RAW_NAF_PATH}"
mkdir -p ${LOCAL_RAW_NAF_PATH}
if [ "$?" -ne 0 ]; then
  echo "  Unable to create local RAW_NAF directory"
  exit 1
fi
gsutil -m -o GSUtil:parallel_process_count=1 -o GSUtil:parallel_thread_count=24 cp -n -r  ${GCS_DATE_SOURCE} ${LOCAL_RAW_NAF_PATH}
if [ "$?" -ne 0 ]; then
  echo "  Unable to download records data locally from ${GCS_DATE_SOURCE}"
  display_usage
  exit 1
fi
echo "  Downloaded records from ${GCS_DATE_SOURCE}"

################################################################################
# Generates a valid NAF file with new line to process as input stream
################################################################################
LOCAL_NAF_PATH=./data/naf/${DS}
echo "Generates the NAF file ${LOCAL_NAF_PATH} in local disk"
LOCAL_NAF_FILE=${LOCAL_NAF_PATH}/${DS}.naf
echo "Creating local naf directory"
mkdir -p ${LOCAL_NAF_PATH}
if [ "$?" -ne 0 ]; then
  echo "  Unable to create local NAF directory"
  exit 1
fi
#TODO missing validation (detects //SR and //ER if not avoid it)
for data in ${LOCAL_RAW_NAF_PATH}/${DS}/*.data; do (cat $data && echo) >> ${LOCAL_NAF_FILE}; done
if [ "$?" -ne 0 ]; then
  echo "  Unable to validate data and save it to NAF file ${LOCAL_NAF_FILE}"
  display_usage
  exit 1
fi
echo "  Validated data saved in ${LOCAL_NAF_FILE}"

################################################################################
# Convert format from NAF to CSV files
################################################################################
echo "Converting NAF files to csv format"
LOCAL_CSV_PATH=./data/csv
LOCAL_CSV_FILE=${LOCAL_CSV_PATH}/${DS}.csv
echo "Creating local csv directory"
mkdir -p ${LOCAL_CSV_PATH}
if [ "$?" -ne 0 ]; then
  echo "  Unable to create local CSV directory"
  exit 1
fi
echo "Converting NAF messages to csv format"
cat ${LOCAL_NAF_FILE} | python -m pipe_naf_reader.naf_parser > ${LOCAL_CSV_FILE}
if [ "$?" -ne 0 ]; then
  echo "  Unable to convert records from NAF to CSV format"
  display_usage
  exit 1
fi
echo "  Coverted records from NAF to CSV"

################################################################################
# Uploads local csv files to GCS
################################################################################
echo "Uploads local CSV ${LOCAL_CSV_FILE} to remote path ${GCS_CSV_FILE}"
gsutil cp ${LOCAL_CSV_FILE} ${GCS_CSV_FILE}
if [ "$?" -ne 0 ]; then
  echo "  Unable to upload local CSV file ${LOCAL_CSV_FILE} to remote path ${GCS_CSV_FILE}"
  display_usage
  exit 1
fi
echo "  Uploaded CSV file to ${GCS_CSV_FILE}"
fi # end SKIP_PROCESSING conditional

################################################################################
# Uploads from GCS to Big Query
################################################################################
BQ_OUTPUT_PATH=${BQ_OUTPUT}_${DS//-/}
BQ_PATTERN="^[a-zA-Z0-9_\-]+[\.:][a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+$"
if [[ "${BQ_OUTPUT_PATH}" =~ ${BQ_PATTERN} ]]; then
  # if colon punctuation is not present replace only the first dot with colon punctuation.
  BQ_OUTPUT_COLON=$(if [[ ${BQ_OUTPUT_PATH} != *":"*  ]]; then echo ${BQ_OUTPUT_PATH/./:}; else echo ${BQ_OUTPUT_PATH}; fi)
else
  echo "Error passing the BQ_OUTPUT it should match the following pattern (${BQ_PATTERN})."
  exit 1
fi
SCHEMA=${ASSETS}/naf-schema.json
echo "Loads BQ table ${BQ_OUTPUT_COLON} from CSV file ${GCS_CSV_FILE}"
bq load \
  --field_delimiter "," \
  --skip_leading_rows 1 \
  --source_format=CSV \
  ${BQ_OUTPUT_COLON} \
  "${GCS_CSV_FILE}" \
  ${SCHEMA}
if [ "$?" -ne 0 ]; then
  echo "  Unable to upload to BigQuery ${BQ_OUTPUT_COLON}"
  display_usage
  exit 1
fi
echo "  Uploaded to BigQuery in table ${BQ_OUTPUT_COLON}"

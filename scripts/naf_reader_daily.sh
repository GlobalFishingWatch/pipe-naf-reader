#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
LIB=${THIS_SCRIPT_DIR}/../pipe_naf_reader
source ${THIS_SCRIPT_DIR}/pipeline.sh

PROCESS=$(basename $0 .sh)
ARGS=( NAME \
  GCS_SOURCE \
  GCS_CSV_OUTPUT \
  BQ_OUTPUT \
  DS \
  SCHEMA_FILE_NAME )

echo -e "\nRunning:\n${PROCESS}.sh $@ \n"

display_usage() {
  echo -e "\nUsage:\n${PROCESS}.sh ${ARGS[*]}\n"
  echo -e "NAME: Name of the country that gives the NAF files.\n"
  echo -e "GCS_SOUCE: Source of the GCS where the NAF files are stored (Format expected gs://<BUCKET>/<OBJECT>).\n"
  echo -e "GCS_CSV_OUTPUT: Folder where to store the CSVs output result from the NAF parser (Format expected gs://<BUCKET>/<OBJECT>).\n"
  echo -e "BQ_OUTPUT: BigQuery dataset and table where will be stored the output (Format expected <DATASET>.<TABLE>).\n"
  echo -e "DS: The date expressed with the following format YYYY-MM-DD. To be used for request.\n"
  echo -e "SCHEMA_FILE_NAME: The schema file name to be appropriate for that country (<NAME_OF_FILE>).\n"
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
cat ${LOCAL_NAF_FILE} | python -m pipe_naf_reader.naf_parser --name ${SCHEMA_FILE_NAME} > ${LOCAL_CSV_FILE}
if [ "$?" -ne 0 ]; then
  echo "  Unable to convert records from NAF to CSV format"
  display_usage
  exit 1
fi
echo "  Coverted records from NAF to CSV"

################################################################################
# Uploads local csv files to GCS
################################################################################
GCS_CSV_FILE=${GCS_CSV_OUTPUT}/${DS}.csv
echo "Uploads local CSV ${LOCAL_CSV_FILE} to remote path ${GCS_CSV_FILE}"
gsutil cp ${LOCAL_CSV_FILE} ${GCS_CSV_FILE}
if [ "$?" -ne 0 ]; then
  echo "  Unable to upload local CSV file ${LOCAL_CSV_FILE} to remote path ${GCS_CSV_FILE}"
  display_usage
  exit 1
fi
echo "  Uploaded CSV file to ${GCS_CSV_FILE}"

################################################################################
# Uploads from GCS to Big Query
################################################################################
echo "Uploads CSV file in remote location ${GCS_CSV_FILE}"
BQ_PATH=${BQ_OUTPUT}_${DS//-/}
FIXED_SCHEMA=${ASSETS}/${SCHEMA_FILE_NAME}.json
AUTODETECT_SCHEMA="--autodetect"
SCHEMA=""
if [ -e "${FIXED_SCHEMA}" ]
then
  SCHEMA=${FIXED_SCHEMA}
  AUTODETECT_SCHEMA=""
  echo "  Reading customized schema found in ${SCHEMA} disabling the AUTODETECTION"
fi
bq load ${AUTODETECT_SCHEMA} \
  --field_delimiter "," \
  --skip_leading_rows 1 \
  --source_format=CSV \
  ${BQ_PATH} \
  "${GCS_CSV_FILE}" \
  ${SCHEMA}
if [ "$?" -ne 0 ]; then
  echo "  Unable to upload to BigQuery ${BQ_PATH}"
  display_usage
  exit 1
fi
echo "  Uploaded to BigQuery in table ${BQ_PATH}"

#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
  echo "Available Commands"
  echo "  naf_reader_daily   Reads the NAF files and converts in a csv daily queries."
}


if [[ $# -le 0 ]]
then
  display_usage
  exit 1
fi


case $1 in

  naf_reader_daily)
    ${THIS_SCRIPT_DIR}/naf_reader_daily.sh "${@:2}"
    ;;

  *)
    display_usage
    exit 0
    ;;
esac

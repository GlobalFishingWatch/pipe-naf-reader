#!/usr/bin/env bash

PIPELINE='pipe_naf_reader'
PIPELINE_VERSION=$(python -c "import pkg_resources; print(pkg_resources.get_distribution('${PIPELINE}').version)")

# set array of labels to use in bq commands
LABELS_VERSION="${PIPELINE_VERSION//[^a-zA-Z0-9_\-]/_}"
LABELS=(
  "component:${PIPELINE}"
  "version:${LABELS_VERSION}"
)


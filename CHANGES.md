# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v3.0.1 - 2022-03-27

### Added

* [Data Pipeline/PIPELINE-851](https://globalfishingwatch.atlassian.net/browse/PIPELINE-851): Adds
  support for fields that come empty, `field1///field2/value2` meaning `value1=None`.

## v3.0.0 - 2021-05-27

### Changed

* [Data Pipeline/PIPELINE-406](https://globalfishingwatch.atlassian.net/browse/PIPELINE-406): Changes
  the NAF parser to read only the headers that are declared in the schema, in
  case the NAF file comes with extra headers and a schema is set ignore the
  extra headers.
  Updates pipe-tools to `v3.2.0`.
  Updates the Google SDK to version `342.0.0`.
  Removes unnecessary libraries instalation.

## v2.0.0 - 2021-04-19

### Added

* [Data Pipeline/PIPELINE-246](https://globalfishingwatch.atlassian.net/browse/PIPELINE-246): Adds
  NAF process to ingest Data from Costa Rica.

## v1.0.1 - 2020-09-30

### Added

* [Data Pipeline/PIPELINE-67](https://globalfishingwatch.atlassian.net/browse/PIPELINE-67): Adds
  Tests to cover naf_reader process.
  Documents the python code.

### Changed

* [Data Pipeline/PIPELINE-67](https://globalfishingwatch.atlassian.net/browse/PIPELINE-67): Changes
  the pipe-tools version to `v3.1.2`.
  the Google SDK version to `312.0.0`.

### Removed

* [Data Pipeline/PIPELINE-67](https://globalfishingwatch.atlassian.net/browse/PIPELINE-67): Removes
  Airflow variable `temp_bucket` that was not used.
  Airflow variable `pipeline_bucket` that was not used.
  Airflow variable `pipeline_dataset` that was not used.

## v1.0.0 - 2020-04-13

### Added

* [GlobalFishingWatch/gfw-eng-tasks#55](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/55): Adds
  support for python 3.
  Increase the google sdk version.
  Pipe-tools version increase to `v3.1.1`.

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#55](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/55): Changes
  TimeDeltaSensor object by using end_date parameter when building the DAG.
  KubernetesOperatr instantation by using the build_docker_task inherited method.

## v0.0.5 - 2019-12-05

### Added

* [GFW-Tasks#1164](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1164): Adds
  the source_sensor functionality from DagFactory.

## v0.0.4 - 2019-10-01

### Added

* [GFW-Tasks#1140](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1140): Adds
  the callsign as new column for chilean schema, they started sending it from 9/26.

## v0.0.3 - 2019-06-21

### Changed

* [GFW-Tasks#1063](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1063): Changes
  on clumn, uses external_id field instead of imo field.

## v0.0.2 - 2019-06-18

### Added

* [GFW-Tasks#1063](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1063): Adds
  processor of NAF messages and generate a Partitioned Table with them.
  Normalizes the partitioned table fields.

## v0.0.1 - 2019-06-11

### Added

* [GFW-Tasks#1032](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1032): Adds
  implementation of NAF message parser DAG.

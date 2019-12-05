# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

# pipe-NAF-reader
Countries that currently send `NAF` messages to Global Fishing Watch.
- Chile.
- Panama.
- Costa Rica.
- Papua New Guinea.

These countries are sending us `NAF` messages, we need a DAG pipeline that given a file that contains one NAF file per line it outputs a CSV with a header line and the parsed messages so we can then upload that file to GCS and BQ.

This repo is responsable for reading each `NAF` message of a country and parsing it. Generate a CSV with a header and the messages to upload them to BQ.

The NAF Parser generates a CSV that contains all positional messages for all messages in a source folder. The CSV needs to have a first row the name of the attributes.

The explanation of each field in a NAF format is explaned in the [defaul schema](https://github.com/GlobalFishingWatch/pipe-naf-reader/blob/master/assets/naf-schema.json)

The parser will adjust the header related to the `NAF` message, if not will throw an error:
For example for:
    //SR//AD/PAN//FR/PAN//TM/POS//NA/MEGA 811//IR/47083-PEXT//RC/HP4077//XR/8523632//DA/190515//TI/1151//LT/-20.860//LG/-100.274//FS/PAN//RN/320491//ER

The first line of the CSV will be:
    AD,TM,NA,IR,RC,XR,DA,TI,LT,LG,SP,CO,FS


**Important**
- The field `name` must start and end with an alphanumeric character, could have a `-` as separator.
- The schema will be placed in `assets` folder. The script will try to search for one that has the name of the country as a prefix and in case it exists will be used, in other case the autodetect option will be enabled.


# Example of manual execution

First step: Parse the NAF and prepare the CSV.
```bash
$ docker compose run --rm pipe_naf_reader naf_reader_daily chile-aquaculture gs://scratch_matias/real-time-naf/aquaculture gs://scratch_matias/naf_to_csv/aquaculture scratch_matias_ttl_60_days.naf_chile_aquaculture 2023-10-16 chile-schema
```

Second step: Read the CSV and upload it to BQ.
```bash
$ docker compose run --rm pipe_naf_reader generate_partitioned_table_daily chile "scratch_matias_ttl_60_days.naf_chile" "scratch_matias_ttl_60_days.raw_naf_processed_partitioned" "2023-10-16"```

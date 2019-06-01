# pipe-NAF-reader
Chile and Panama will be sending us NAF messages, we need a DAG pipeline that given a file that contains one NAF file per line it outputs a CSV with a header line and the parsed messages so we can then upload that file to GCS and BQ.


# Requirements

Need to configure the Airflow Variable configurations like this

```json
configurations:[
{name:"panama", gcs_source:"gs://vms-gfw/panama/real-time-naf",gcs_csv_output:"gs://vms-gfw/panama/naf-to-csv",bq_output:"VMS_Panama.raw_naf_messages"},
{name:"chile", gcs_source:"gs://vms-gfw/chile/real-time-naf",gcs_csv_output:"gs://vms-gfw/chile/naf-to-csv",bq_output:"VMS_Chile.raw_naf_messages"}
]
```

# Example of manual execution

```bash
$ ./scripts/naf_reader_daily.sh "panama" "gs://scratch-matias/test/attachments" "gs://scratch-matias/test/panama/naf_to_csv" "scratch_matias.naf_panama" "2019-04-17"```

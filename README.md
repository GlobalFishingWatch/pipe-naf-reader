# pipe-NAF-reader
Chile and Panama will be sending us NAF messages, we need a DAG pipeline that given a file that contains one NAF file per line it outputs a CSV with a header line and the parsed messages so we can then upload that file to GCS and BQ.


# Requirements

Need to configure the Airflow Variable configurations like this

```json
{
  "configurations": [
    {
      "bq_output": "scratch_matias.naf_panama", 
      "gcs_csv_output": "gs://scratch-matias/test/panama/naf_to_csv", 
      "gcs_source": "gs://scratch-matias/test/panama-raw-data/real-time-naf", 
      "name": "panama"
    }, 
    {
      "bq_output": "scratch_matias.naf_chile_aquaculture", 
      "gcs_csv_output": "gs://scratch-matias/test/chile-aquaculture/naf_to_csv", 
      "gcs_source": "gs://scratch-matias/test/chile-raw-data/chile-raw-data-aquaculture/aquaculture", 
      "name": "chile-aquaculture"
    }, 
    {
      "bq_output": "scratch_matias.naf_chile_artesanales", 
      "gcs_csv_output": "gs://scratch-matias/test/chile-artesanales/naf_to_csv", 
      "gcs_source": "gs://scratch-matias/test/chile-raw-data/artesanales", 
      "name": "chile-artesanales"
    }, 
    {
      "bq_output": "scratch_matias.naf_chile_industriales", 
      "gcs_csv_output": "gs://scratch-matias/test/chile-industriales/naf_to_csv", 
      "gcs_source": "gs://scratch-matias/test/chile-raw-data/industriales", 
      "name": "chile-industriales"
    }, 
    {
      "bq_output": "scratch_matias.naf_chile_transportadoras", 
      "gcs_csv_output": "gs://scratch-matias/test/chile-transportadoras/naf_to_csv", 
      "gcs_source": "gs://scratch-matias/test/chile-raw-data/transportadoras", 
      "name": "chile-transportadoras"
    }
  ]
}
```

**Important**
- The field `name` must start and end with an alphanumeric character, could have a `-` as separator.
- The schema will be placed in `assets` folder. The script will try to search for one that has the name of the country as a prefix and in case it exists will be used, in other case the autodetect option will be enabled.


# Example of manual execution

```bash
$ ./scripts/naf_reader_daily.sh "panama" "gs://scratch-matias/test/attachments" "gs://scratch-matias/test/panama/naf_to_csv" "scratch_matias.naf_panama" "2019-04-17"```

# pipe-NAF-reader
Chile and Panama will be sending us NAF messages, we need a DAG pipeline that given a file that contains one NAF file per line it outputs a CSV with a header line and the parsed messages so we can then upload that file to GCS and BQ.

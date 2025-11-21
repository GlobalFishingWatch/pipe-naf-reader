"""
The NAF Parser generates a CSV that contains all positional messages for all
messages in a source folder. The CSV needs to have a first row the name of the
attributes.


The explanation of each field in a NAF forma
can be found in https://www.naf-format.org/field-codes.htm
The `SR` (starts of the message) and `ER` (end of message transmission)
are the limits of the NAF message.

For example for:
    //SR//AD/PAN//FR/PAN//TM/POS//NA/MEGA 811//IR/47083-PEXT//RC/HP4077//XR/8523632//DA/190515//TI/1151//LT/-20.860//LG/-100.274//FS/PAN//RN/320491//ER  # noqa: E501

The first line of the CSV will be:
    AD,TM,NA,IR,RC,XR,DA,TI,LT,LG,SP,CO,FS

"""

import argparse
import csv
import json
import logging
import sys
import time


logger = logging.getLogger(__name__)
NAF_SCHEMA = "./assets/naf-schema.json"
START_RECORD = "SR"
END_RECORD = "ER"


class NAFParser:
    """Process a NAF input stream and converts it to a CSV file."""

    def parse(self, entries):
        """
        Receives a list of entries and returns a header and row array.

        :@param entries: list of lines of CSV file.
        :@type entries: list.
        :@returns: the header list and the row dict.
        """
        start = False
        end = False
        header = []
        row = {}
        for entry in entries:
            pair = entry.split("/")
            if pair[0] == END_RECORD:
                end = True
            if start and not end:
                label = pair[0]
                header.append(label)
                row[label] = self.normalize_value(label, pair[1])
            if pair[0] == START_RECORD:
                start = True
        return header, row

    def normalize_value(self, label, value):
        """
        Normalizes the value.

        :@param label: The label or column name.
        :@type label: str.
        :@param value: The value of the row.
        :@type value: str.
        :@return: the normalized value.
        """
        return value

    def _get_header_from_schema(self, output_stream):
        """
        Returns the csv.DictWriter and the header list from the schema.

        :@param output_stream: The stream where to output the CSV info.
        :@type output_stream: IO.
        :@return: Tuple[csv.DictWriter, List[str]]
            first: the csv writer.
            second: the header list.
        """
        csv_writer = None
        header = []
        with open(NAF_SCHEMA) as schema:
            schema_fields = json.load(schema)
            for schema_field in schema_fields:
                header.append(schema_field["name"])
        csv_writer = csv.DictWriter(
            output_stream,
            fieldnames=header,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )
        csv_writer.writeheader()
        logger.info(f"  Reading the headers from schema {NAF_SCHEMA}")
        return csv_writer, header

    def process(self, input_stream, output_stream):
        """
        Processes the NAF input_stream and outputs the CSV stream.

        :@param input_stream: The NAF data or stdin.
        :@type input_stream: file.
        :@param output_stream: The CSV file or stdout.
        :@type output_stream: file.
        """
        # Load headers from NAF SCHEMA
        csv_writer, schema_headers = self._get_header_from_schema(output_stream)

        for line in input_stream:
            stripped_line = line.strip()
            try:
                if len(stripped_line.split("///")) > 1:
                    logger.warn(f"There are empty fields in line {stripped_line}")
                    splitted = stripped_line.rsplit("//")
                else:
                    splitted = stripped_line.split("//")
                header, row = self.parse(splitted)

                if row is not None:
                    # If a header is not in the schema_headers it's removed
                    # from the row keys to make it compatible with the schema
                    list(
                        map(
                            lambda y: row.pop(y, None),
                            filter(lambda x: x not in schema_headers, header),
                        )
                    )
                    csv_writer.writerow(row)
            except Exception as err:
                logger.error(err)
                logger.error(f"Unable to convert NAF message to csv row at {stripped_line}")
                # if it is not valid just exclude it.
                exit(1)


# -------------- MAIN RECEPTION --------------------

if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Parses NAF messages uploads to GCS and BQ.")
    parser.add_argument(
        "--input_stream",
        help="NAF message input",
        required=False if not sys.stdin.isatty() else True,
    )
    parser.add_argument("--output_stream", help="CSV file output", required=False)
    args = parser.parse_args()

    if args.input_stream is None:
        args.input_stream = sys.stdin
    if args.output_stream is None:
        args.output_stream = sys.stdout

    naf_parser = NAFParser()
    naf_parser.process(**vars(args))
    logger.info(f"Execution time {(time.time()-start_time)/60} minutes")

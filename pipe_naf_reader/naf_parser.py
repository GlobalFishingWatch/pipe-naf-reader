"""
The NAF Parser generates a CSV that contains all positional messages for all messages in a source folder. The CSV needs to have a first row the name of the attributes.


The explanation of each field in a NAF format will be
    SR starts of the message
    AD name of the source (PAN for PANAMA, FR for FRANCIA)
    TM transmission method
    NA name of the vessel
    IR internal registry
    RC radio letter
    XR IMO number (for local vessels some are repeated in radio letter, because these do not have IMO)
    DA date (YYMMDD)
    TI time (hh:mm)
    LT latitude
    LG longitude
    SP speed (#.#)
    CO course
    FS flag
    ER end of message transmission

For example for:
    //SR//AD/PAN//FR/PAN//TM/POS//NA/MEGA 811//IR/47083-PEXT//RC/HP4077//XR/8523632//DA/190515//TI/1151//LT/-20.860//LG/-100.274//FS/PAN//RN/320491//ER

The first line of the CSV will be:
    AD,TM,NA,IR,RC,XR,DA,TI,LT,LG,SP,CO,FS

"""
from datetime import datetime
import argparse
import csv
import json
import logging
import os.path
import sys
import time

"""Process a NAF input strams and converts it to a CSV file."""
class NAFParser():

    """
    Receives a list of entries and returns a header and row array.

    :@param entries: list of lines of CSV file.
    :@type entries: list.
    :@returns: the header list and the row dict.
    """
    def parse(self, entries):
        start=False
        end=False
        header=[]
        row={}
        for entry in entries:
                pair=entry.split('/')
                if pair[0]=='ER':
                        end=True
                if start and not end:
                        label = pair[0]
                        header.append(label)
                        row[label] = self.normalize_value(label, pair[1])
                if pair[0]=='SR':
                        start=True
        return header,row

    """
    Normalizes the value.

    :@param label: The label or column name.
    :@type label: str.
    :@param value: The value of the row.
    :@type value: str.
    :@return: the normalized value.
    """
    def normalize_value(self, label, value):
        return value

    """
    Loads the customized schema and prepares the csv writer object.

    :@param name: The country name.
    :@type name: str.
    :@param output_stream: The stream where to output the CSV info.
    :@type output_stream: str.
    :@return: The csv writer object.
    """
    def _loads_customized_schema(self, name, output_stream):
        csv_writer = None
        #Reads custom schema in case it exists
        customized_schema_path = './assets/{}.json'.format(name)
        if os.path.isfile(customized_schema_path):
            header = []
            with open(customized_schema_path) as customized_schema:
                schema_fields = json.load(customized_schema)
                for schema_field in schema_fields:
                    header.append(schema_field['name'])
            csv_writer = csv.DictWriter(output_stream, fieldnames=header, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writeheader()
            logging.info("  Reading the headers from customized schema {}".format(customized_schema_path))
        return csv_writer


    """
    Processes the NAF input_stream and outputs the CSV stream.

    :@param name: The country name.
    :@type name: str.
    :@param input_stream: The NAF data or stdin.
    :@type input_stream: file.
    :@param output_stream: The CSV file or stdout.
    :@type output_stream: file.
    """
    def process(self, name, input_stream, output_stream):
        # with open(output_file, "wb+") as f:
        csv_writer = self._loads_customized_schema(name, output_stream)

        for line in input_stream:
            stripped_line = line.strip()
            try:
                splitted = stripped_line.split('//')
                header, row = self.parse(splitted)
                if csv_writer is None:
                    csv_writer = csv.DictWriter(output_stream, fieldnames=header, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    csv_writer.writeheader()
                if row:
                    csv_writer.writerow(row)
            except Exception as err:
                logging.error(err)
                logging.error("Unable to convert NAF message to csv row at {}".format(stripped_line))
                #if it is not valid just exclude it.
                exit(1)
            except:
                logging.error('There was an error parsing the line <{}>', stripped_line)
                exit(1)

#-------------- MAIN RECEPTION --------------------

if __name__ == '__main__':
    # start_time = time.time()
    parser = argparse.ArgumentParser(description='Parses NAF messages uploads to GCS and BQ.')
    parser.add_argument('--name', help='The country name.', required=True)
    parser.add_argument('--input_stream', help='NAF message input', required=False if not sys.stdin.isatty() else True)
    parser.add_argument('--output_stream', help='CSV file output', required=False)
    args = parser.parse_args()

    if args.input_stream is None:
        args.input_stream = sys.stdin
    if args.output_stream is None:
        args.output_stream = sys.stdout

    naf_parser = NAFParser()
    naf_parser.process(**vars(args))
    # print("Execution time {0} minutes".format((time.time()-start_time)/60))

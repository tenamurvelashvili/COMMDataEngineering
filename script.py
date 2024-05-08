import argparse
import os

import processData

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--source_file', required=True, type=str, help='source file name with extention (extension '
                                                                         'must be csv)')
parser.add_argument('-e', '--encodings', type=str, help='enter encoding of source file',
                    choices=["utf-8", "latin1"])
parser.add_argument('-p', '--partition_key', nargs="+", type=str, help='partition key from source file columns name',
                    choices=['type', 'title', 'director', 'cast', 'country', 'date_added', 'release_year', 'rating',
                             'duration', 'listed_in', 'description'], required=True)
parser.add_argument('-op', '--output_file_path', required=True, type=str, help='output file path')
parser.add_argument('-ot', '--output_file_type', required=True, type=str, help='output file type',
                    choices=['csv', 'excel', 'json'])

args = parser.parse_args()
_, file_extension = os.path.splitext(args.source_file)
if not file_extension.endswith(".csv"):
    raise ValueError("file extension must be .csv")

if args.encodings is None:
    confirmation = input("Enter encoding (utf-8 = 0 , latin1 = 1) ")
    if confirmation == "0":
        args.encodings = "utf-8"
    elif confirmation == "1":
        args.encodings = "latin1"
    else:
        raise ValueError("Invalid encoding entered")

source_file = args.source_file
encodings = args.encodings
partition_key = args.partition_key
output_file_path = args.output_file_path
output_file_type = args.output_file_type

print(source_file, encodings, partition_key, output_file_path, output_file_type)
processData.run(source_file, encodings, partition_key, output_file_path, output_file_type)

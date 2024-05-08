import argparse

import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('--chunk_size', '-ch', type=int, default=100, required=True)
parser.add_argument('--partitioning_key', '-partition', type=str , action='append', required=True)
parser.add_argument('--filename', type=str, default="netflix_titles.csv")
parser.add_argument('--file_path', type=str, default="data/")
parser.add_argument('--file_mode', choices=['w', 'a'], required=True)

args = parser.parse_args()
chunk_size = args.chunk_size
partitioning_key = args.partitioning_key
filename = args.filename
file_path = args.file_path
file_mode = args.file_mode


def partitioning(chunk_size, filename, partitioning_key, file_path, mode):
    distinct_list = set()

    for chunk in pd.read_csv(filename, chunksize=100, encoding='latin1'):
        for partition in partitioning_key:
            distinct_list.update(chunk[partitioning_key].unique())
            print(chunk[partitioning_key].unique())
            for value in distinct_list:
                filtered_df = chunk[chunk[partitioning_key] == value]
                if (mode == "a"):
                    filtered_df.to_csv(f"{file_path}{value}_file.csv", mode='a', header=False, index=False)
                elif (mode == "w"):
                    filtered_df.to_csv(f"{file_path}{value}_file.csv", index=False)
                yield filtered_df


for tv_chunk in partitioning(chunk_size, filename, partitioning_key, file_path, file_mode):
    print(list(tv_chunk))

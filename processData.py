import pandas as pd


def process_dataframe(df: pd.DataFrame, partitioning_key: list, output_file_path: str, output_file_type: str) -> None:
    """
    Process the given dataframe based on the partitioning key, and create files with the specified file type and path.

    :param df: The dataframe to process.
    :type df: pd.DataFrame
    :param partitioning_key: The list of column names to use for partitioning.
    :type partitioning_key: list
    :param output_file_path: The path where the output files will be created.
    :type output_file_path: str
    :param output_file_type: The file type of the output files (e.g., "csv", "txt").
    :type output_file_type: str
    :return: None
    """
    def format_data(key):
        df[key] = df[key].astype(str)

    list(map(format_data, partitioning_key))
    df['filter_key'] = df[partitioning_key].agg(''.join, axis=1)
    unique_filter_columns = df['filter_key'].drop_duplicates()

    for _, row in unique_filter_columns.items():
        result_dataframe = df.loc[(df['filter_key'] == row)]
        create_file(output_file_type, output_file_path, result_dataframe, row)


def create_file(file_type: str, file_path, result_dataframe: pd.DataFrame, row) -> None:
    if file_type == 'csv':
        result_dataframe.to_csv(f'{file_path}{row}.csv', mode='w', header=True, index=False)
    elif file_type == 'excel':
        writer = pd.ExcelWriter(f'{file_path}{row}.xlsx', engine='xlsxwriter')
        result_dataframe.to_excel(writer, sheet_name='Sheet1', header=True, index=False)
        writer.close()
    elif file_type == 'json':
        result_dataframe.to_json(f'{file_path}{row}.json', mode='w', orient='columns')
    else:
        raise ValueError(f'file type {file_type} not supported')


def run(source_file, encoding, partition_key, output_file_path, output_file_type):
    df = pd.read_csv(source_file, encoding=encoding)
    process_dataframe(df, partition_key, output_file_path, output_file_type)

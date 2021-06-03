import csv
import glob
import os
import multiprocessing

import pandas as pd
from pandas import DataFrame


def clean_output(input_file):
    df = pd.read_csv(input_file)
    DF_RM_DUP = df.drop_duplicates(subset=' vertices')
    column = DF_RM_DUP[' vertices']
    cell_values = []
    for cell in column:
        values = cell.split()
        nodes = []
        for number in values:
            nodes.append(int(number))
        nodes.sort()
        cell_values.append(nodes)
    temp_df = DataFrame(cell_values)
    no_duplicates_df = temp_df.drop_duplicates()

    output_file_name = input_file[:8]
    output_file_name = output_file_name + "_cleaned.csv"
    no_duplicates_df.to_csv("cleaned/"+output_file_name, index=False)


def join_files():
    os.chdir("cleaned/")
    cleaned_files = glob.glob("*.csv")
    for cleaned_file in cleaned_files:
        with open(cleaned_file, 'r') as input_file:
            with open("JoinedFile.csv", 'a+') as output_file:
                next(input_file)
                for line in input_file:
                    output_file.write(line)


def create_csvs_to_process():
    joined_file = open("JoinedFile.csv")
    reader = csv.reader(joined_file)
    number_of_rows = len(list(reader))
    number_of_cpus = multiprocessing.cpu_count()
    rows_per_csv = int(number_of_rows / number_of_cpus)
    os.mkdir("split")
    split_count = 1
    with open("JoinedFile.csv", 'r') as input_file:
        written_rows = 0
        while written_rows < number_of_rows:
            splitted_file_name = "split" + str(split_count) + ".csv"
            split_count += 1
            rows_count = 0
            with open("split/" + splitted_file_name, 'a+') as output_file:
                for line in input_file:
                    output_file.write(line)
                    rows_count += 1
                    if rows_count == rows_per_csv:
                        break
                written_rows += rows_count


os.chdir(".")
os.mkdir("cleaned")
files = glob.glob("*.csv")
for file in files:
    clean_output(file)
join_files()
create_csvs_to_process()
import argparse, os, zipfile
import polars as pl
from pathlib import Path

def clean_deed(filename, input_dir):
    # filepaths
    input_filepath = input_dir + "/raw_parquet/" + filename
    output_dir = input_dir + "/" + "cleaned_parquet"
    output_filepath = input_dir + "/" + "cleaned_parquet" + filename

    # check if parquet already exists, if it does, skip
    if os.path.exists(output_filepath):
        print("Cleaned parquet file already exists. Skipping this file in the directory...")
        return

    # access contents of the parquet file
    df = pl.read_parquet(Path(input_filepath))
    print(df.schema)

    #convert to parquet
    print("Converting to parquet...")
    (pl.scan_parquet(Path(unzipped_filepath), 
                 separator = '|', 
                 low_memory = True, 
                 try_parse_dates=True, 
                 infer_schema_length=1000, 
                 ignore_errors = True, 
                 truncate_ragged_lines = True) 
            ).sink_parquet(Path(output_filepath), compression="snappy")
    print("Parquet file complete.")


    # print("Parquet file complete.")

    # #delete unzipped file for memory conservation
    # print("Deleting unzipped txt file...")
    # os.remove(unzipped_filepath)
    # print("Complete. Moving to next file...")

def main(input_dir: str):

    print("Collecting all files in input directory...")
    # get all files within the directory
    raw_parquet_dir = input_dir + "/" + "raw_parquet"
    filenames = (file for file in os.listdir(raw_parquet_dir) 
         if os.path.isfile(os.path.join(raw_parquet_dir, file)))

    # create output directory if it doesn't exist
    output_dir = input_dir + "/" + "cleaned_parquet"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    deed_files = [filename for filename in filenames if "Deed" in filename]

    print("Looping through all files...")
    for file in deed_files:
        print("Starting to process", file, "...")
        clean_deed(filename=file, input_dir=input_dir)
    print("Done.")

def setup(args=None):
    parser = argparse.ArgumentParser(description='Clean & subset raw parquet files into files that are ready to be joined.')
    parser.add_argument('--input_dir', required=True, type=str, dest="input_dir", help="Path to input directory.")
    return parser.parse_args(args)

if __name__ == "__main__":
    main(**vars(setup()))

# python clean_parquet.py --input_dir dev-data/raw_small
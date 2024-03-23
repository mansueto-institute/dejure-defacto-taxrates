import argparse, os, zipfile
import polars as pl
from pathlib import Path

def convert_sales(filename, input_dir):
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/" + filename
    output_dir = input_dir + "/" + "parquet"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")
    output_filepath_ranked = output_dir + "/ranked_" + filename.replace(".txt.zip", ".parquet")

    # # check if parquet already exists, if it does, skip
    # if os.path.exists(output_filepath):
    #     print("Parquet file already exists. Skipping this file in the directory...")
    #     return
    
    # decompress file
    print("Unzipping file...")
    with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
        zip_ref.extractall(unzipped_dir)
    unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")

    #convert to parquet
    print("Converting to parquet...")

    # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L158-L213
    (pl.scan_csv(Path(unzipped_filepath), separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
            .select(['PropertyID', 'SaleAmt', 'RecordingDate', 'FIPS', 'FATimeStamp', 'FATransactionID', 'TransactionType', 'SaleDate'])
                .filter(pl.col('PropertyID').is_not_null())
                .filter((pl.col('SaleAmt') > 0) & (pl.col('SaleAmt').is_not_null()))
                .with_columns(pl.col('RecordingDate').cast(pl.Utf8).str.slice(offset=0,length = 4).alias("RecordingYearSlice"))
                .with_columns([
                    (pl.col('PropertyID').cast(pl.Int64)),
                    (pl.col('FIPS').cast(pl.Utf8).str.pad_start(5, "0")),
                    (pl.col('RecordingDate').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                    (pl.col('SaleDate').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                    (pl.col('FATimeStamp').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                    (pl.col('FATransactionID').cast(pl.Utf8).str.slice(offset=0,length = 1).alias("FATransactionID_1")),
                    (pl.when(pl.col('TransactionType').cast(pl.Utf8).is_in(['1', '2', '3', '4', '5', '6'])).then(pl.col('TransactionType').cast(pl.Utf8)).otherwise(None).name.keep()),
                    ])
                .with_columns([
                    (pl.col("RecordingDate").dt.year().alias("RecordingYear")),
                    (pl.col('SaleDate').dt.year().alias("SaleYear")),
                    (pl.col('FATimeStamp').dt.year().alias("FATimeStampYear")),
                    (pl.when((pl.col("FATransactionID_1").is_in(['1', '6'])) & (pl.col('TransactionType').is_in(['2', '3']))).then(1).otherwise(0).alias("SaleFlag"))
                ])                     
            ).sink_parquet(Path(output_filepath), compression="snappy")

    # (pl.scan_parquet(Path(input_filepath), low_memory = True)
    #     .filter(pl.col('SaleFlag') == 1)
    #     .with_columns([
    #         (pl.coalesce(pl.col(["SaleYear", "RecordingYear"])).cast(pl.Int16).alias("SaleRecordingYear")),
    #         (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(['RecordingYear', "PropertyID"]).alias("RecentSaleByYear")),
    #         (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(["PropertyID"]).alias("MostRecentSale")),
    #     ])
    #     .filter(pl.col('RecentSaleByYear') == 1)
    #     ).collect(streaming=True)
    
    # # sale_egress.write_parquet(file = Path(input_dir) / sale_2_parquet, use_pyarrow=True, compression="snappy")
    # # sale_egress.clear()

    print("Parquet file complete.")

    #delete unzipped file for memory conservation
    print("Deleting unzipped txt file...")
    os.remove(unzipped_filepath)
    print("Complete. Moving to next file...")

def main(input_dir: str):

    print("Collecting all files in input directory...")
    # get all files within the directory
    filenames = (file for file in os.listdir(input_dir) 
         if os.path.isfile(os.path.join(input_dir, file)))

    # create output directory if it doesn't exist
    output_dir = input_dir + "/" + "parquet"
    unzipped_dir = input_dir + "/" + "unzipped"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(unzipped_dir):
        os.makedirs(unzipped_dir)

    sorted_filenames = {}
    sorted_filenames["Deed"] = [filename for filename in filenames if "Deed" in filename]
    sorted_filenames["Prop"] = [filename for filename in filenames if "Prop" in filename]
    sorted_filenames["TaxHist"] = [filename for filename in filenames if "TaxHist" in filename]
    sorted_filenames["ValHist"] = [filename for filename in filenames if "ValHist" in filename]

    print("Looping through all files...")
    # convert each file to parquet [PARRALELIZE THIS?]
    for type, list in sorted_filenames.items():
        if type == "Deed":
            for filename in list:
                convert_sales(filename, input_dir)
    print("Done.")

def setup(args=None):
    parser = argparse.ArgumentParser(description='Convert zipped txt input files to parquet files.')
    parser.add_argument('--input_dir', required=True, type=str, dest="input_dir", help="Path to input directory.")
    return parser.parse_args(args)

if __name__ == "__main__":
    main(**vars(setup()))

# python txt_to_parquet.py --input_dir dev-data/raw_small
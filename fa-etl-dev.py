import argparse, os, zipfile
from pathlib import Path
import logging
import psutil

import polars as pl

def mem_profile() -> str:
    """
    Return memory usage, str  [Function written by Nico]
    """
    mem_use = str(round(100 - psutil.virtual_memory().percent,4))+'% of '+str(round(psutil.virtual_memory().total/1e+9,3))+' GB RAM'
    return mem_use

def convert_sales(filename, input_dir):
    '''
    Convert zipped txt sales (deed) file into parquet format.

    Inputs:
    - filename: str ("Deed36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to)
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    '''
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/" + filename
    output_dir = input_dir + "/" + "parquet"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")
    output_filepath_ranked = output_dir + "/ranked_" + filename.replace(".txt.zip", ".parquet")

    # skip conversion if the file already exists
    if os.path.exists(output_filepath):
        logging.info(f"{output_filepath} already exists. Skipping this file in the directory...")
        return
    if os.path.exists(output_filepath_ranked):
        logging.info(f"{output_filepath_ranked} already exists. Skipping this file in the directory...")
        return
    
    # decompress file
    logging.info("Unzipping file...")
    with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
        zip_ref.extractall(unzipped_dir)
    unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")

    #convert all sales txt file to parquet
    # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L158-L213
    try:
        logging.info(f"Converting {input_filepath} to parquet...")
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
        logging.info(f"{output_filepath} complete.")
    except Exception as e:
        os.remove(output_filepath)
        logging.info(f"Error: {str(e)}")

    # ranked sales file
    try:
        logging.info(f"Creating {output_filepath_ranked}...")
        sale_ranked = (pl.scan_parquet(Path(output_filepath), low_memory = True, parallel='row_groups', use_statistics=False, hive_partitioning=False)
            .filter(pl.col('SaleFlag') == 1)
            .with_columns([
                #(pl.coalesce(pl.col(["SaleYear", "RecordingYear"])).cast(pl.Int16).alias("SaleRecordingYear")),
                (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(['RecordingYear', "PropertyID"]).alias("RecentSaleByYear")),
                (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(["PropertyID"]).alias("MostRecentSale")),
            ])
            .filter(pl.col('RecentSaleByYear') == 1)
            ).select(['PropertyID', 'SaleAmt', 'RecordingYear']
            ).collect(streaming=True)

        sale_ranked.write_parquet(Path(output_filepath_ranked), use_pyarrow=True, compression="snappy")
        sale_ranked.clear()
        logging.info(f"{output_filepath_ranked} complete.")
    except Exception as e:
        os.remove(output_filepath_ranked)
        logging.info(f"Error: {str(e)}")

    #delete unzipped file for memory conservation
    logging.info("Deleting unzipped txt file...")
    os.remove(unzipped_filepath)
    logging.info("Complete. Moving to next file...")

def convert_prop(filename, input_dir):
    '''
    Convert zipped txt prop (annual) file into parquet format.

    Inputs:
    - filename: str ("Prop36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to)
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    '''
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/" + filename
    output_dir = input_dir + "/" + "parquet"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")

    # check if parquet already exists, if it does, skip
    if os.path.exists(output_filepath):
        logging.info(f"{output_filepath} already exists. Skipping this file in the directory...")
        return
    
    # decompress file
    logging.info("Unzipping file...")
    with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
        zip_ref.extractall(unzipped_dir)
    unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")

    # convert annual file to parquet
    logging.info(f"Converting {input_filepath} to parquet...")
    # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L127-L155
    (pl.scan_csv(unzipped_filepath, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
        .select(['PropertyID', 'PropertyClassID', "FATimeStamp", 'SitusLatitude', 'SitusLongitude', 'SitusFullStreetAddress', 'SitusCity', 'SitusState', 'SitusZIP5', 'FIPS', 'SitusCensusTract', 'SitusCensusBlock', 'SitusGeoStatusCode'])
            #.filter(pl.col('PropertyClassID') == 'R')
            .filter(pl.col('PropertyID').is_not_null())
            .with_columns([
                (pl.col('PropertyID').cast(pl.Int64)),
                (pl.col("FATimeStamp").cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                (pl.when((pl.col('SitusLatitude') == 0)).then(None).otherwise(pl.col('SitusLatitude')).alias('SitusLatitude')),
                (pl.when((pl.col('SitusLongitude') == 0)).then(None).otherwise(pl.col('SitusLongitude')).alias('SitusLongitude')),
                (pl.col('FIPS').cast(pl.Utf8).str.pad_start(5, "0")),
                (pl.col('SitusCensusTract').cast(pl.Utf8).str.pad_start(6, "0")),
                (pl.col('SitusCensusBlock').cast(pl.Utf8).str.pad_start(4, "0")),
                (pl.col('SitusZIP5').cast(pl.Utf8).str.pad_start(5, "0")),
                (pl.when(pl.col('SitusGeoStatusCode').cast(pl.Utf8).is_in(['5', '7', '9', 'A', 'B', 'X', 'R'])).then(pl.col('SitusGeoStatusCode')).otherwise(None).name.keep()),
                (pl.when(pl.col('PropertyClassID').cast(pl.Utf8).is_in(['R', 'C', 'O', 'F', 'I', 'T', 'A', 'V', 'E'])).then(pl.col('PropertyClassID')).otherwise(None).name.keep()),
                (pl.concat_str([pl.col("FIPS"), pl.col('SitusCensusTract')], separator= "_").fill_null(pl.col('FIPS')).alias("FIPS_SitusCensusTract"))
                ])
        ).sink_parquet(Path(output_filepath), compression="snappy")
    logging.info(f"{output_filepath} complete.")

    #delete unzipped file for memory conservation
    logging.info("Deleting unzipped txt file...")
    os.remove(unzipped_filepath)
    logging.info("Complete. Moving to next file...")



def convert_taxhist(filename, input_dir):
    '''
    Convert zipped txt tax history file into parquet format.

    Inputs:
    - filename: str ("TaxHist36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to)
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    '''

    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/" + filename
    output_dir = input_dir + "/" + "parquet"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")

    # check if parquet already exists, if it does, skip
    if os.path.exists(output_filepath):
        logging.info(f"{output_filepath} already exists. Skipping this file in the directory...")
        return

    # decompress file
    logging.info("Unzipping file...")
    with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
        zip_ref.extractall(unzipped_dir)
    unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")
    
    # convert taxhist file to parquet
    logging.info(f"Converting {input_filepath} to parquet...")
    # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L127-L155
    (pl.scan_csv(unzipped_filepath, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
        .select(['PropertyID', 'TaxYear', 'TaxAmt'])
        .with_columns([
            (pl.col('PropertyID').cast(pl.Int64)),
            (pl.col('TaxYear').cast(pl.Int64)),
            (pl.col('TaxAmt').cast(pl.Int64)),
        ])
        ).sink_parquet(Path(output_filepath), compression="snappy")
    logging.info(f"{output_filepath} complete.")

    #delete unzipped file for memory conservation
    logging.info("Deleting unzipped txt file...")
    os.remove(unzipped_filepath)
    logging.info("Complete. Moving to next file...")


def convert_valhist(filename, input_dir):
    '''
    Convert zipped txt value history file into parquet format.

    Inputs:
    - filename: str ("ValHist36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to)
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    '''
    # filepaths
    unzipped_dir = input_dir+ "/" +'unzipped'
    input_filepath = input_dir + "/" + filename
    output_dir = input_dir + "/" + "parquet"
    output_filepath = output_dir + "/" + filename.replace(".txt.zip", ".parquet")

    # check if parquet already exists, if it does, skip
    if os.path.exists(output_filepath):
        logging.info(f"{output_filepath} already exists. Skipping this file in the directory...")
        return

    # decompress file
    logging.info("Unzipping file...")
    with zipfile.ZipFile(input_filepath, 'r') as zip_ref:
        zip_ref.extractall(unzipped_dir)
    unzipped_filepath = unzipped_dir + "/" + filename.replace(".txt.zip", ".txt")
    
    # convert valhist file to parquet
    logging.info(f"Converting {input_filepath} to parquet...")
    # see https://github.com/mansueto-institute/fa-etl/blob/main/fa-etl.py#L127-L155
    (pl.scan_csv(unzipped_filepath, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)
        .select(['PropertyID', 'AssdTotalValue', 'AssdYear', 'MarketTotalValue', 'MarketValueYear', 'ApprTotalValue', 'ApprYear', 'TaxableYear'])
        .with_columns([
            (pl.col('PropertyID').cast(pl.Int64)),
            (pl.col('AssdTotalValue').cast(pl.Int64)),
            (pl.col('AssdYear').cast(pl.Int64)),
            (pl.col('MarketTotalValue').cast(pl.Int64)),
            (pl.col('MarketValueYear').cast(pl.Int64)),
            (pl.col('ApprTotalValue').cast(pl.Int64)),
            (pl.col('ApprYear').cast(pl.Int64)),
            (pl.col('TaxableYear').cast(pl.Int64)),
        ])
        ).sink_parquet(Path(output_filepath), compression="snappy")
    logging.info(f"{output_filepath} complete.")

    #delete unzipped file for memory conservation
    logging.info("Deleting unzipped txt file...")
    os.remove(unzipped_filepath)
    logging.info("Complete. Moving to next file...")


def join(input_dir, valhist_filename, prop_filename, ranked_deed_filename):
    '''
    Creates one merged parquet file with an observation as each unique
    combination of property id/year present in the value history file, 
    left joining in:
    - prop/annual file (property characterstics: geography, property class)
    - 

    Inputs:
    - filename: str ("TaxHist36061.txt.zip")
    - input_dir: str, path to directory where the file exists (and where all other files will be saved to)
    
    Returns: Nothing. Saves parquet file to a parquet subdirectory within the input_dir.
    '''


    #read in parquet as lazy Dataframes
    logging.info(f"Reading in parquet files to merge...")
    valhist = pl.scan_parquet(Path(input_dir+"/parquet/"+valhist_filename))
    prop = pl.scan_parquet(Path(input_dir+"/parquet/"+prop_filename))
    ranked_deed = pl.scan_parquet(Path(input_dir+"/parquet/"+ranked_deed_filename))
    #taxhist = pl.scan_parquet(taxhist_path)

    #set output path
    output_filepath = Path(input_dir+"/parquet/merged.parquet")

    # https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.join.html
    valhist.join(
        # first join in the data from the annual file (prop characteristics)
        other= prop,
        on= ['PropertyID'],
        #validate='m:1', #checks if only 1 propertyid in annual file
        force_parallel=True

    # ).join(
    #     # second join in the data from the most recent sale of each year
    #     other=ranked_deed,
    #     left_on=['PropertyID', ''],
    #     right_on=['PropertyID', ''],
    #     force_parallel=True

    ).sink_parquet(output_filepath, compression="snappy")


def main(input_dir: str, log_file: str):
    '''
    @TODO: add doc string
    '''
    # set up file environment
    output_dir = input_dir + "/" + "parquet"
    unzipped_dir = input_dir + "/" + "unzipped"
    deployments_dir = input_dir + "/" + "deployments"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(unzipped_dir):
        os.makedirs(unzipped_dir)
    if not os.path.exists(deployments_dir):
        os.makedirs(deployments_dir)
    Path(log_file).touch()
    
    # set up logging
    logging.basicConfig(filename=Path(log_file), format='%(asctime)s:%(message)s: ', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logging.captureWarnings(True)
    logging.info(f'Starting script. Memory usage {mem_profile()}')

    # get all files within the directory and sort into a dictionary
    logging.info("Collecting all files in input directory...")
    filenames = [file for file in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, file))]
    sorted_filenames = {}
    for file_type in ["Prop", "Deed", "TaxHist", "ValHist"]:
        sorted_filenames[file_type] = [filename for filename in filenames if file_type in filename]
    logging.info(f'Files to process: {sorted_filenames}')

    # @TODO: check to make sure there is one of each of 4 files to continue? better test here?

    # convert each file to parquet [PARRALELIZE THIS?]
    logging.info("Looping through all files...")
    for type, list in sorted_filenames.items():
        if type == "Deed":
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_sales(filename, input_dir)
        if type == "Prop":
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_prop(filename, input_dir)
        if type == "TaxHist":
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_taxhist(filename, input_dir)
        if type == "ValHist":
            for filename in list:
                logging.info(f'Processing {filename}. Memory usage {mem_profile()}')
                convert_valhist(filename, input_dir)

    #left merge all 3 to valhist on propertyID and year
    
    
    #join()


    #clean up
    #delete empty unzipped folder
    os.rmdir(unzipped_dir)

    logging.info(f'Memory usage {mem_profile()}')
    logging.info("Done.")





def setup(args=None):
    '''
    @TODO: add doc string
    '''
    parser = argparse.ArgumentParser(description='Convert zipped txt input files to parquet files.')
    parser.add_argument('--input_dir', required=True, type=str, dest="input_dir", help="Path to input directory.")
    parser.add_argument('--log_file', required=True, type=str, dest="log_file", help="Path to log file.")
    return parser.parse_args(args)

if __name__ == "__main__":
    main(**vars(setup()))

# sample line of code to run the scipt
# python fa-etl-dev.py --input_dir dev-data/raw_48203 --log_file dev-data/raw_48203/deployments/deployment.log

import polars as pl
import pyarrow as pa
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import numpy as np
import dask.dataframe as dd
import dask_geopandas
import pygeohash

import time
import re

import sys
import psutil
import logging
import os
from pathlib import Path
import argparse

from pandas._libs.lib import is_integer
import math

#pl.Config.set_tbl_rows(10)
#pl.Config.set_tbl_cols(200)
#pl.Config.set_fmt_str_lengths(100)

def weighted_qcut(values, weights, q, **kwargs):
    """
    Return weighted quantile cuts from a given series, values.
    """
    if is_integer(q):
        quantiles = np.linspace(0, 1, q + 1)
    else:
        quantiles = q
    order = weights.iloc[values.argsort()].cumsum()
    bins = pd.cut(order / order.iloc[-1], quantiles, **kwargs)
    return bins.sort_index()

def mem_profile() -> str:
    """
    Return memory usage, str
    """
    mem_use = str(round(100 - psutil.virtual_memory().percent,4))+'% of '+str(round(psutil.virtual_memory().total/1e+9,3))+' GB RAM'
    return mem_use


def main(log_file: Path, input_dir: Path, annual_file: str, sale_file: str, historical_file: str, xwalk_dir: Path, xwalk_files: list, output_dir: Path):

    logging.basicConfig(filename=Path(log_file), format='%(asctime)s:%(message)s: ', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

# %%
# input_dir = '/Users/nm/Downloads/firstamerican/nationwide-files'
# annual_file = '20220606_Annual.txt'
# sale_file = '20220606_DeedMtg.txt'
# historical_file = '20220606_ValueHistory.parquet'

# input_dir = '/Users/nm/Downloads/test/input'
# annual_file = 'Prop01001.txt'
# sale_file = 'Deed01001.txt'
# historical_file = 'ValHist01001.txt'
# hpi_file = 'county_hpi.parquet'
#
# output_dir = '/Users/nm/Downloads/test/output'
#
# xwalk_dir = '/Users/nm/Downloads/geos/xwalks'
# xwalk_files = ['zcta.parquet', 'block_groups_2020.parquet', 'city_places.parquet', 'urban_classifications.parquet', 'unified_school_districts.parquet', 'elementary_school_districts.parquet', 'secondary_school_districts.parquet', 'congressional_districts_118.parquet']
#
# try: os.remove(path = '/Users/nm/Downloads/test/input/Prop01001.parquet')
# except: pass
# try: os.remove(path = '/Users/nm/Downloads/test/input/Deed01001.parquet')
# except: pass
# try: os.remove(path = '/Users/nm/Downloads/test/input/ValHist01001.parquet')
# except: pass
# try: os.remove(path = '/Users/nm/Downloads/test/output/crosswalk.parquet')
# except: pass
# try: os.remove(path = Path(output_dir) / 'timeseries_av_sales.parquet')
# except: pass
# try: os.remove(path = Path(output_dir) / 'timeseries_av_sales.parquet')
# except: pass
# try: os.remove(path = Path(output_dir) / 'sales_res_av_all.parquet')
# except: pass

    logging.info('Starting script.')
    logging.info('Memory usage: ' + mem_profile())
    os.makedirs(output_dir, exist_ok = True)

    annual_parquet = re.sub('.txt', '.parquet', annual_file)
    sale_parquet = re.sub('.txt', '.parquet', sale_file)
    sale_2_parquet = 'ranked_' + sale_parquet
    historical_parquet = re.sub('.txt', '.parquet', historical_file)

    logging.info(f'Check if files exist.')
    if (Path(input_dir) / annual_parquet).exists():
        print(Path(input_dir) / annual_parquet)
        annual_ingest = (pl.scan_parquet(Path(input_dir) / annual_parquet))
    elif (Path(input_dir) / annual_file).exists():
        annual_ingest = (pl.scan_csv(Path(input_dir) / annual_file, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)) 
    else:
        logging.info(f'No annual file found.')
        sys.exit()

    if (Path(input_dir) / sale_parquet).exists():
        sale_ingest = (pl.scan_parquet(Path(input_dir) / sale_parquet))
    elif (Path(input_dir) / sale_file).exists():
        sale_ingest = (pl.scan_csv(Path(input_dir) / sale_file, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)) 
    else:
        logging.info(f'No sale file found.')
        sys.exit()

    if (Path(input_dir) / historical_parquet).exists():
        historical_ingest = (pl.scan_parquet(Path(input_dir) / historical_parquet))
    elif (Path(input_dir) / historical_file).exists():
        historical_ingest = (pl.scan_csv(Path(input_dir) / historical_file, separator = '|', low_memory = True, try_parse_dates=True, infer_schema_length=1000, ignore_errors = True, truncate_ragged_lines = True)) 
    else:
        logging.info(f'No historical file found.')
        sys.exit()

    logging.info('Memory usage: ' + mem_profile())
    if not (Path(input_dir) / annual_parquet).exists():
        logging.info(f'Sinking {annual_file}.')
        t0 = time.time()
        ((annual_ingest
            .with_columns([
                (pl.col('PropertyID').cast(pl.Utf8)),
                (pl.col('FIPS').cast(pl.Utf8).str.rjust(5, "0")),
                (pl.col("FATimeStamp").cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                (pl.col('CurrentSaleRecordingDate').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                (pl.col("AssdYear").cast(pl.Int16)),
                (pl.col("MarketYear").cast(pl.Int16)),
                (pl.col("TaxYear").cast(pl.Int16)),
                ((pl.col("TaxAmt") / 100).keep_name()),
                (pl.when(pl.col('OwnerOccupied').cast(pl.Utf8).is_in(['B', 'O', 'Y'])).then(True).otherwise(None).keep_name()),
                (pl.when((pl.col('SitusLatitude') == 0)).then(None).otherwise(pl.col('SitusLatitude')).alias('SitusLatitude')),
                (pl.when((pl.col('SitusLongitude') == 0)).then(None).otherwise(pl.col('SitusLongitude')).alias('SitusLongitude')),
                (pl.col('SitusCensusTract').cast(pl.Utf8).str.rjust(6, "0")),
                (pl.col('SitusCensusBlock').cast(pl.Utf8).str.rjust(4, "0")),
                (pl.when(pl.col('SitusGeoStatusCode').cast(pl.Utf8).is_in(['5', '7', '9', 'A', 'B', 'X', 'R'])).then(pl.col('SitusGeoStatusCode')).otherwise(None).keep_name()),
                (pl.when(pl.col('PropertyClassID').cast(pl.Utf8).is_in(['R', 'C', 'O', 'F', 'I', 'T', 'A', 'V', 'E'])).then(pl.col('PropertyClassID')).otherwise(None).keep_name()),
                (pl.when((pl.col('LandUseCode') >= 10) & (pl.col('LandUseCode') <= 9309)).then(pl.col('LandUseCode')).otherwise(None).keep_name()),
                (pl.concat_str([pl.col("FIPS"), pl.col('SitusCensusTract')], separator= "_").fill_null(pl.col('FIPS')).alias("FIPS_SitusCensusTract"))
                ])
            .with_columns([
                (pl.col('CurrentSaleRecordingDate').dt.year().alias("CurrentSaleRecordingYear"))
                ])
        ).sink_parquet(Path(input_dir) / annual_parquet, compression="snappy"))
        t1 = time.time()
        logging.info(f"{annual_file} sink successful. {round((t1-t0)/60,2)} minutes.")

    logging.info('Memory usage: ' + mem_profile())
    if not (Path(input_dir) / sale_parquet).exists():
        logging.info(f'Writing {sale_file}.')
        t0 = time.time()
        ((sale_ingest
            .with_columns(pl.col('RecordingDate').cast(pl.Utf8).str.slice(offset=0,length = 4).alias("RecordingYearSlice"))
            .with_columns([
                ## Format date fields
                (pl.col('PropertyID').cast(pl.Utf8)),
                (pl.col('FIPS').cast(pl.Utf8).str.rjust(5, "0")),
                (pl.col("RecordingDate").cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                (pl.col('SaleDate').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                (pl.col('FATimeStamp').cast(pl.Utf8).str.to_date("%Y%m%d", strict = False, exact = False)),
                (pl.col('FATransactionID').cast(pl.Utf8).str.slice(offset=0,length = 1).alias("FATransactionID_1")),
                (pl.when(pl.col('MatchedFlag') == 'Y').then(True).otherwise(None).keep_name()),
                (pl.when(pl.col('ResaleFlag') == 'Y').then(True).otherwise(None).keep_name()),
                (pl.when(pl.col('NewConstructionFlag') == 'Y').then(True).otherwise(None).keep_name()),
                (pl.when(pl.col('InterFamilyFlag') == 'Y').then(True).otherwise(None).keep_name()),
                (pl.when(pl.col('ForeclosureFlag') == 'Y').then(True).otherwise(None).keep_name()),
                (pl.when(pl.col('ReoSaleFlag') == 'Y').then(True).otherwise(None).keep_name()),
                (pl.when(pl.col('ArmsLengthFlag') == 'T').then(True).when(pl.col('ArmsLengthFlag') == 'F').then(False).otherwise(None).keep_name()),
                (pl.when(pl.col('PartialInterestFlag').cast(pl.Utf8).is_in(['1', '2', '3', '4', '5', '6', '7', '8', '9'])).then(pl.col('PartialInterestFlag').cast(pl.Utf8)).otherwise(None).keep_name()),
                (pl.when(pl.col('DocumentType').cast(pl.Utf8).is_in(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'K', 'DT', 'L', 'M', 'N', 'O'])).then(pl.col('DocumentType').cast(pl.Utf8)).otherwise(None).keep_name()),
                (pl.when(pl.col('TransactionType').cast(pl.Utf8).is_in(['1', '2', '3', '4', '5', '6'])).then(pl.col('TransactionType').cast(pl.Utf8)).otherwise(None).keep_name()),
                (pl.when(pl.col('SalesPriceCode').cast(pl.Utf8).is_in(['2', '4', '5', '6', '8', '9', '11', '15', '18', '20', '23', '26', '28', '31', '33', '34', '36', '41', '42', '1', '3', '7', '10', '12', '13', '14', '16', '17', '19', '21', '22', '24', '25', '27', '29', '30', '32', '35', '37', '38', '39', '40', '43', '44'])).then(pl.col('SalesPriceCode').cast(pl.Utf8)).otherwise(None).keep_name()),
                (pl.when(pl.col('FirstMtgLoanTransType').cast(pl.Utf8).is_in(['1', '2', '3', '4', '5', '6'])).then(pl.col('FirstMtgLoanTransType').cast(pl.Utf8)).otherwise(None).keep_name()),
                (pl.when(pl.col('FirstMtgLoanType').cast(pl.Utf8).is_in(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34'])).then(pl.col('FirstMtgLoanType').cast(pl.Utf8)).otherwise(None).keep_name())
                ])
            .with_columns([
                (pl.col("RecordingDate").dt.year().alias("RecordingYear")),
                (pl.col('SaleDate').dt.year().alias("SaleYear")),
                (pl.col('FATimeStamp').dt.year().alias("FATimeStampYear"))
            ])
            .select(['PropertyID', "RecordingDate", 'SaleAmt', 'SalesPriceCode', 'TransferTax', 'FATransactionID', 'FirstMtgAmt', 'TransactionType', 'FATransactionID_1', 'RecordingYear', 'SaleYear'])
            .filter((pl.col("RecordingYear") >= 2019) | (pl.col("SaleYear") >= 2019))
        ).sink_parquet(Path(input_dir) / sale_parquet, compression="snappy"))
        t1 = time.time()
        logging.info(f"{sale_file} write successful. {round((t1-t0)/60,2)} minutes.")

    logging.info('Memory usage: ' + mem_profile())
    if not (Path(input_dir) / sale_2_parquet).exists():
        t0 = time.time()
        sale_egress = (pl.scan_parquet(Path(input_dir) / sale_parquet, low_memory = True)
            # Requires collection to memory
            .with_columns([
                (pl.coalesce(pl.col(["SaleYear", "RecordingYear"])).cast(pl.Int16).alias("SaleRecordingYear")),
                (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(['RecordingYear', "PropertyID"]).alias("RecentSaleByYear")),
                (pl.col("RecordingDate").rank(method="random", descending = True, seed = 1).over(["PropertyID"]).alias("MostRecentSale")),
                (pl.when((pl.col("FATransactionID_1").is_in(['1', '6'])) & (pl.col('TransactionType').is_in(['2', '3']))).then(1).otherwise(0).alias("SaleFlag"))
            ])
            ).collect(streaming=True)

        sale_egress.write_parquet(file = Path(input_dir) / sale_2_parquet, use_pyarrow=True, compression="snappy")
        sale_egress.clear()
        #sink_parquet(Path(input_dir) / sale_parquet, compression="snappy")
        t1 = time.time()
        logging.info(f"{sale_2_parquet} write successful. {round((t1-t0)/60,2)} minutes.")

    logging.info('Memory usage: ' + mem_profile())
    if not (Path(input_dir) / historical_parquet).exists():
        logging.info(f'Sinking {historical_file}.')
        t0 = time.time()
        (historical_ingest
            .with_columns([
                (pl.col('PropertyID').cast(pl.Utf8)),
                (pl.col('FIPS').cast(pl.Utf8).str.rjust(5, "0")),
                (pl.col("AssdYear").cast(pl.Int16)),
                (pl.col("MarketValueYear").cast(pl.Int16)),
                (pl.col("ApprYear").cast(pl.Int16)),
                (pl.col("TaxableYear").cast(pl.Int16))
            ])
            ).sink_parquet(Path(input_dir) / historical_parquet, compression="snappy")
        t1 = time.time()
        logging.info(f"{historical_parquet} sink successful. {round((t1-t0)/60,2)} minutes.")

    logging.info('Memory usage: ' + mem_profile())
    logging.info(f'Converting parcel locations to geometries.')

    block_group_data = gpd.read_parquet(path= Path(xwalk_dir) / 'block_groups_2020.parquet').to_crs(4326)
    block_group_data = block_group_data[['block_group_fips', 'lon', 'lat']].rename(columns={'lon': 'lon_bg', 'lat': "lat_bg"})

    tract_2020_data = gpd.read_parquet(path= Path(xwalk_dir) / 'tracts_2020.parquet').to_crs(4326)
    tract_2020_data = tract_2020_data[['tract_fips', 'lon', 'lat']].rename(columns={'lon': 'lon_t', 'lat': "lat_t"})

    tract_2010_data = pd.read_parquet(path= Path(xwalk_dir) / 'tracts_2010.parquet')
    tract_2010_data = tract_2010_data[['tract_fips_2010', 'lon_10', 'lat_10']].rename(columns={'lon_10': 'lon_t_10', 'lat_10': "lat_t_10"})

    county_cbsa_data = pd.read_parquet(path= Path(xwalk_dir) / 'county_cbsa_fips.parquet')

    if not (Path(output_dir) / 'crosswalk.parquet').exists():
        t0 = time.time()

        chunk_data = (pl.scan_parquet(Path(input_dir) / annual_parquet)
                        .select(["FIPS_SitusCensusTract"])
                        .group_by(["FIPS_SitusCensusTract"])
                        .agg(pl.count())
                        ).collect(streaming=True)
        chunk_data = chunk_data.to_pandas()
        count_per_partition = 300000
        chunk_data = chunk_data.sort_values(by=["FIPS_SitusCensusTract"], ascending=True).reset_index(drop=True)
        cuts = math.ceil(chunk_data.agg({'count': 'sum'}).reset_index()[0][0]/count_per_partition)
        chunk_data['partition_index'] = weighted_qcut(chunk_data["FIPS_SitusCensusTract"], chunk_data['count'], cuts, labels=False)
        chunk_dict = chunk_data[["FIPS_SitusCensusTract",'partition_index']].drop_duplicates().reset_index(drop = True).groupby('partition_index')["FIPS_SitusCensusTract"].apply(list).to_dict()

        for i in chunk_dict.items():
            chunk_list = i[1]
            print(chunk_list)
            logging.info(f'Processing: {chunk_list}')
            # parcel_data = (pl.scan_parquet(Path(input_dir) / annual_parquet, low_memory = True)
            #                  .select(['PropertyID','APN','FIPS','SitusCensusTract', 'SitusCensusBlock', 'SitusLongitude','SitusLatitude'])
            #                  .filter(pl.col('FIPS') == i)).collect(streaming=True)
            # parcel_data = parcel_data.to_pandas()
            parcel_data = pd.read_parquet(Path(input_dir) / annual_parquet, columns=['PropertyID','APN','FIPS','SitusCensusTract', 'SitusCensusBlock', 'SitusLongitude','SitusLatitude'], 
                                            memory_map = True, filters = [("FIPS_SitusCensusTract", 'in', chunk_list)])

            # parcel_data = pd.read_parquet(Path(input_dir) / annual_parquet, columns=['PropertyID','APN','FIPS','SitusCensusTract', 'SitusCensusBlock', 'SitusLongitude','SitusLatitude'])
            parcel_data['BlockGroupFIPS'] = (parcel_data['FIPS'].astype(str) + parcel_data['SitusCensusTract'].astype(str) + parcel_data['SitusCensusBlock'].astype(str)).str.slice(start=0, stop=12)
            parcel_data['TractFIPS'] = (parcel_data['FIPS'].astype(str) + parcel_data['SitusCensusTract'].astype(str)).str.slice(start=0, stop=11)
            parcel_data = parcel_data.merge(block_group_data, how = 'left', left_on = 'BlockGroupFIPS', right_on = 'block_group_fips')
            parcel_data = parcel_data.merge(tract_2020_data, how = 'left', left_on = 'TractFIPS', right_on = 'tract_fips')
            parcel_data = parcel_data.merge(tract_2010_data, how = 'left', left_on = 'TractFIPS', right_on = 'tract_fips_2010')
            parcel_data['SitusLongitude'] = (parcel_data['SitusLongitude'].combine_first(parcel_data['lon_bg']).combine_first(parcel_data['lon_t']).combine_first(parcel_data['lon_t_10']))
            parcel_data['SitusLatitude'] = (parcel_data['SitusLatitude'].combine_first(parcel_data['lat_bg']).combine_first(parcel_data['lat_t']).combine_first(parcel_data['lat_t_10']))
            parcel_data = parcel_data.merge(county_cbsa_data, how = 'left', left_on = 'FIPS', right_on = 'county_fips')

            # parcel_data = dd.from_pandas(parcel_data[['PropertyID','APN','FIPS','SitusLongitude','SitusLatitude', 'state_codes', 'state_fips', 'state_name', 'county_code', 'county_name', 'county_fips', 'cbsa_fips', 'cbsa_title', 'area_type', 'central_outlying_county', 'county_pop20']], chunksize = 100000)
            # parcel_data = dask_geopandas.from_dask_dataframe(parcel_data, geometry=dask_geopandas.points_from_xy(parcel_data['SitusLongitude'] , parcel_data['SitusLatitude'], crs="EPSG:4326"))

            parcel_data = gpd.GeoDataFrame(parcel_data[['PropertyID','APN','FIPS','SitusLongitude','SitusLatitude',
                                                        'state_codes', 'state_fips', 'state_name', 'county_code', 'county_name', 'county_fips', 'cbsa_fips', 'cbsa_title', 'area_type', 'central_outlying_county', 'county_pop20'
                                                        ]],
                                            geometry = gpd.points_from_xy(parcel_data['SitusLongitude'] , parcel_data['SitusLatitude'], crs="EPSG:4326"))

            for xwalk in xwalk_files:
                print(xwalk)
                logging.info(f'        {xwalk}')
                geolabel=gpd.read_parquet(path= Path(xwalk_dir) / xwalk).to_crs(4326)
                geolabel[re.sub('.parquet', '', xwalk) + '_area_m2'] = round(geolabel['geometry'].to_crs(3395).area*1e-6,5)
                parcel_data = gpd.sjoin(left_df = parcel_data, right_df = geolabel, how='left', predicate='within').drop(['index_right'], axis=1)
                # parcel_data = parcel_data.sjoin(df = geolabel, how='left', predicate='intersects').drop(['index_right'], axis=1).compute()

            parcel_data = parcel_data[~parcel_data.geometry.is_empty]
            parcel_data = parcel_data.assign(valid_geometry=1)
            parcel_data['geohash'] = list(map(lambda x: pygeohash.encode(x.x, x.y, precision=18), parcel_data.geometry.make_valid().to_list()))

            parcel_data = parcel_data.drop(['geometry'], axis=1)
            #parcel_data = parcel_data.assign(county_fips = parcel_data['block_group_fips'].str.slice(start=0, stop=5))
            #parcel_data = parcel_data.assign(state_fips = parcel_data['block_group_fips'].str.slice(start=0, stop=2))
            parcel_data = dd.from_pandas(data = parcel_data, npartitions = 1)
            dd.to_parquet(df = parcel_data, path = Path(output_dir) / f'crosswalk_dataset.parquet', append=True, ignore_divisions=True)

        logging.info('Memory usage: ' + mem_profile())
        logging.info(f'Writing crosswalk.parquet')
        #crosswalk_data = dd.read_parquet(path = Path(output_dir) / 'crosswalk_dataset.parquet').compute()
        #crosswalk_data = pl.from_pandas(crosswalk_data)
        #crosswalk_data.write_parquet(file = Path(output_dir) / 'crosswalk.parquet', use_pyarrow=True, compression="snappy")
        (pl.scan_parquet(Path(output_dir) / 'crosswalk_dataset.parquet/*')
            .sink_parquet(Path(output_dir) / 'crosswalk.parquet', compression="snappy"))

        #parcel_data.to_parquet(path= Path(output_dir) / 'crosswalk.parquet', index = False)
        t1 = time.time()
        logging.info(f"crosswalk.parquet created. {round((t1-t0)/60,2)} minutes.")
        del parcel_data

    logging.info('Memory usage: ' + mem_profile())
    logging.info(f"Scanning {sale_2_parquet}.")
    sale_cols = ['PropertyID', 'SaleRecordingYear', 'SaleAmt', 'SalesPriceCode', 'TransferTax', 'FATransactionID', 'FirstMtgAmt', 'FATransactionID_1', 'RecordingYear', 'SaleYear', 'RecentSaleByYear', 'MostRecentSale', 'SaleFlag']
    sales_data = (pl.scan_parquet(Path(input_dir) / sale_2_parquet, low_memory = True)
                        .select(sale_cols)
                        .filter(pl.col('PropertyID').is_not_null())
                        .filter(pl.col('SaleRecordingYear') >= 2019)
                        .filter(pl.col("SaleFlag") == 1)
                        .filter(pl.col('RecentSaleByYear') == 1)
                        .filter((pl.col("SaleAmt").is_not_null()) & (pl.col("SaleAmt") > 0))
                    )
                        #  .filter((pl.col("TaxAmt").is_not_null()) & 

    # sales_count_data = (pl.scan_parquet(Path(input_dir) / sale_2_parquet, low_memory = True)
    #                     .filter((pl.col('PropertyID').is_not_null()))
    #                     .filter(pl.col('SaleRecordingYear') >= 2019)
    #                     .filter(pl.col("SaleFlag") == 1)
    #                     .filter(pl.col('RecentSaleByYear') == 1)
    #                     .group_by(['PropertyID'])
    #                     .agg([pl.count().alias("SaleYearCount")]))

    logging.info(f"Scanning {annual_parquet}.")
    annual_tax_cols = ['PropertyID', 'APN', 'APNSeqNbr', 'PropertyClassID', 'LandUseCode', 'FIPS', 'TaxYear', 'TaxAmt',  "CurrentSaleRecordingYear", "CurrentSalesPrice"]
    annual_tax_data = (pl.scan_parquet(Path(input_dir) / annual_parquet, low_memory = True)
                                .select(annual_tax_cols)
                                .filter((pl.col('PropertyID').is_not_null()))
                                .filter(pl.col("PropertyClassID") == 'R')
                                .filter((pl.col('TaxAmt').is_not_null()) & (pl.col('TaxAmt') > 0))
                            )

    # annual_cols_static = ['PropertyID', 'APN', 'APNSeqNbr', 'PropertyClassID', 'LandUseCode', 'FIPS', 'TaxYear', 'TaxAmt',  "CurrentSaleRecordingYear", "CurrentSalesPrice"]
    # # 'SitusLatitude', 'SitusLongitude', 'SitusCensusTract', 'SitusCensusBlock',
    # # 'LotSizeDepthFeet', 'LotSizeAcres',  'BuildingAreaInd',  'Bedrooms', 'BathTotalCalc', 
    # # 'LotSizeSqFt', 'BuildingArea', 'SumBuildingSqFt', 'TotalRooms', 'SumResidentialUnits', 'SumBuildingsNbr', 
    # annual_static_data = (pl.scan_parquet(Path(input_dir) / annual_parquet, low_memory = True)
    #                             .select(annual_cols_static)
    #                             .filter((pl.col('PropertyID').is_not_null()))
    #                             .filter(pl.col("PropertyClassID") == 'R')
    #                             .filter((pl.col('TaxAmt').is_not_null()) & (pl.col('TaxAmt') > 0))
    #                             .rename({'TaxAmt': 'TaxAmt_AnnualStatic', 'TaxYear': 'TaxYear_AnnualStatic'})
    #                         )

    # annual_cols_tax_ts = ['PropertyID', 'TaxYear', 'TaxAmt']
    # annual_tax_ts_data = (pl.scan_parquet(Path(input_dir) / annual_parquet, low_memory = True)
    #                             .filter(pl.col("PropertyClassID") == 'R')
    #                             .select(annual_cols_tax_ts)
    #                             .filter((pl.col('PropertyID').is_not_null()))
    #                             .filter((pl.col('TaxAmt').is_not_null()) & (pl.col('TaxAmt') > 0))
    #                             #.rename({'TaxAmt': 'TaxAmt_Annual'})
    #                         )

    # annual_cols_assd_ts = ['PropertyID', 'AssdYear', 'AssdTotalValue', 'AssdLandValue', 'AssdImprovementValue']
    # annual_assd_ts_data = (pl.scan_parquet(Path(input_dir) / annual_parquet, low_memory = True)
    #                             .select(annual_cols_assd_ts)
    #                             .filter((pl.col('PropertyID').is_not_null()) & (pl.col('AssdYear').is_not_null()) & (pl.col('AssdTotalValue').is_not_null()) & (pl.col('AssdTotalValue') > 0))
    #                             .rename({"AssdTotalValue" : "AssdTotalValue_Annual", "AssdLandValue": "AssdLandValue_Annual", "AssdImprovementValue": "AssdImprovementValue_Annual"})
    #                         )

    # annual_cols_market_ts = ['PropertyID', 'MarketYear', 'MarketTotalValue', 'MarketValueLand', 'MarketValueImprovement']
    # annual_market_ts_data = (pl.scan_parquet(Path(input_dir) / annual_parquet, low_memory = True)
    #                             .select(annual_cols_market_ts)
    #                             .filter((pl.col('PropertyID').is_not_null()) & (pl.col('MarketYear').is_not_null()) & (pl.col('MarketTotalValue').is_not_null()) & (pl.col('MarketTotalValue') > 0))
    #                             .rename({'MarketValueLand': 'MarketValueLand_Annual', 'MarketValueImprovement': 'MarketValueImprovement_Annual', 'MarketTotalValue': 'MarketTotalValue_Annual'})
    #                         )

    # logging.info(f"Scanning {historical_parquet}.")
    # historical_cols_assd_ts = ['PropertyID', 'AssdYear', 'AssdLandValue', 'AssdImprovementValue', 'AssdTotalValue']
    # historical_assd_data = (pl.scan_parquet(Path(input_dir) / historical_parquet, low_memory = True)
    #                         .filter(pl.col('AssdYear') >= 2017)
    #                         .select(historical_cols_assd_ts)
    #                         .filter((pl.col('PropertyID').is_not_null()) & (pl.col('AssdYear').is_not_null()) & (pl.col('AssdTotalValue').is_not_null()) & (pl.col('AssdTotalValue') > 0))
    #                         .rename({"AssdTotalValue" : "AssdTotalValue_Historical", "AssdLandValue": "AssdLandValue_Historical", "AssdImprovementValue": "AssdImprovementValue_Historical"})
    #                         )

    # historical_cols_tax_ts = ['PropertyID', 'AssdYear', 'TaxValueLand', 'TaxValueImprovement', 'TaxTotalValue']
    # historical_tax_data = (pl.scan_parquet(Path(input_dir) / historical_parquet, low_memory = True)
    #                         .filter(pl.col('AssdYear') >= 2019)
    #                         .select(historical_cols_tax_ts)
    #                         .filter((pl.col('PropertyID').is_not_null()) & (pl.col('AssdYear').is_not_null()) & (pl.col('TaxTotalValue').is_not_null()) & (pl.col('TaxTotalValue') > 0))
    #                         .rename({'TaxValueLand': 'TaxValueLand_Historical', 'TaxValueImprovement': 'TaxValueImprovement_Historical', 'TaxTotalValue': 'TaxTotalValue_Historical'})
    #                         )

    # historical_cols_appr_ts = ['PropertyID', 'ApprYear', 'ApprLandValue', 'ApprImprovementValue', 'ApprTotalValue']
    # historical_appr_data = (pl.scan_parquet(Path(input_dir) / historical_parquet, low_memory = True)
    #                         .filter(pl.col('ApprYear') >= 2017)
    #                         .select(historical_cols_appr_ts)
    #                         .filter((pl.col('PropertyID').is_not_null()) & (pl.col('ApprYear').is_not_null()) & (pl.col('ApprTotalValue').is_not_null()) & (pl.col('ApprTotalValue') > 0))
    #                         .rename({'ApprLandValue': 'ApprLandValue_Historical', 'ApprImprovementValue': 'ApprImprovementValue_Historical', 'ApprTotalValue': 'ApprTotalValue_Historical'})
    #                         )

    # historical_cols_market_ts = ['PropertyID', 'MarketValueYear', 'MarketValueLand', 'MarketValueImprovement', 'MarketTotalValue']
    # historical_market_data = (pl.scan_parquet(Path(input_dir) / historical_parquet, low_memory = True)
    #                         .filter(pl.col('MarketValueYear') >= 2017)
    #                         .select(historical_cols_market_ts)
    #                         .filter((pl.col('PropertyID').is_not_null()) & (pl.col('MarketValueYear').is_not_null()) & (pl.col('MarketTotalValue').is_not_null()) & (pl.col('MarketTotalValue') > 0))
    #                         .rename({'MarketValueLand': 'MarketValueLand_Historical', 'MarketValueImprovement': 'MarketValueImprovement_Historical', 'MarketTotalValue': 'MarketTotalValue_Historical'})
    #                         )

    logging.info(f"Scanning crosswalk.parquet.")
    xwalk_data = (pl.scan_parquet(Path(output_dir) / 'crosswalk.parquet', low_memory = True)
                            .select(pl.exclude(['APN', 'FIPS', 'geometry']))
                            .rename({'SitusLongitude' : 'SitusLongitude_imputed', 'SitusLatitude' : 'SitusLatitude_imputed'})
                            )

    hpi_data = (pl.scan_parquet(Path(xwalk_dir) / 'county_hpi.parquet', low_memory = True)
                            .with_columns([(pl.col('year').cast(pl.Int16))])
                            )

    logging.info(f"Creating unified sales data.")

    logging.info('Memory usage: ' + mem_profile())
    logging.info(f"Scanning join.")
    #sales_unified = sales_data.join(historical_tax_data, left_on=['PropertyID','SaleRecordingYear'], right_on =['PropertyID','AssdYear'], how="left")
    ##sales_unified = sales_unified.join(historical_assd_data, left_on=['PropertyID','SaleRecordingYear'], right_on =['PropertyID','AssdYear'], how="left")
    ##sales_unified = sales_unified.join(historical_market_data, left_on=['PropertyID','SaleRecordingYear'], right_on =['PropertyID','MarketValueYear'], how="left")
    ##sales_unified = sales_unified.join(historical_appr_data, left_on=['PropertyID','SaleRecordingYear'], right_on =['PropertyID','ApprYear'], how="left")
    ##sales_unified = sales_unified.join(sales_count_data, left_on='PropertyID', right_on ='PropertyID', how="left")

    # logging.info(f"Sinking timeseries_av_sales.parquet")
    # if not (Path(output_dir) / 'timeseries_av_sales.parquet').exists():
    #     (sales_data
    #         .join(annual_static_data, left_on='PropertyID', right_on ='PropertyID', how="inner")
    #         .join(annual_tax_ts_data, left_on=['PropertyID', 'SaleRecordingYear'], right_on =['PropertyID', 'TaxYear'], how="left")
    #         .join(xwalk_data, left_on='PropertyID', right_on ='PropertyID', how="left")
    #         .join(hpi_data.select(['county_fips','year','hpi_index_2021']), left_on=['FIPS','SaleRecordingYear'], right_on =['county_fips','year'], how="left")
    #     ).sink_parquet(Path(output_dir) / 'timeseries_av_sales.parquet', compression="snappy")
    # ##sales_unified = sales_unified.join(annual_assd_ts_data, left_on=['PropertyID', 'SaleRecordingYear'], right_on =['PropertyID', 'AssdYear'], how="left")
    # ##sales_unified = sales_unified.join(annual_market_ts_data, left_on=['PropertyID', 'SaleRecordingYear'], right_on =['PropertyID', 'MarketYear'], how="left")
    # logging.info(f"Sink timeseries_av_sales.parquet successful")
    # logging.info('Memory usage: ' + mem_profile())

    logging.info(f"Sinking sales_snapshot_staging.parquet")
    if not (Path(output_dir) / 'sales_snapshot_staging.parquet').exists():
        (sales_data
            .join(annual_tax_data, left_on=['PropertyID', 'SaleRecordingYear'], right_on =['PropertyID', 'TaxYear'], how="inner")
            .join(xwalk_data, left_on='PropertyID', right_on ='PropertyID', how="left")
            .join(hpi_data.select(['county_fips','year','hpi_index_2021']), left_on=['FIPS','SaleRecordingYear'], right_on =['county_fips','year'], how="left")
        ).sink_parquet(Path(output_dir) / 'sales_snapshot_staging.parquet', compression="snappy")
    logging.info(f"Sink sales_snapshot_staging.parquet successful")
    logging.info('Memory usage: ' + mem_profile())

    logging.info(f"Sinking sales_pooled_staging.parquet")
    if not (Path(output_dir) / 'sales_pooled_staging.parquet').exists():
        (sales_data
            .filter(pl.col('MostRecentSale') == 1)
            .join(annual_tax_data, left_on='PropertyID', right_on ='PropertyID', how="inner")
            .join(xwalk_data, left_on='PropertyID', right_on ='PropertyID', how="left")
            .join(hpi_data.select(['county_fips','year','hpi_index_2021']), left_on=['FIPS','SaleRecordingYear'], right_on =['county_fips','year'], how="left")
        ).sink_parquet(Path(output_dir) / 'sales_pooled_staging.parquet', compression="snappy")
    logging.info(f"Sink sales_pooled_staging.parquet successful")
    logging.info('Memory usage: ' + mem_profile())

    # Places where market value should be used instead of assd value
    # KS, NC, PA, NY, TX, SC, KY, AZ
    # ['20', '37', '42', '36', '48', '45', '21', '40']
    # ['20121', '37065', '42101', '36061', '48203', '48255', '48191', '45041']

    os.makedirs(Path(output_dir) / 'agg_pooled', exist_ok = True)
    os.makedirs(Path(output_dir) / 'agg_snapshot', exist_ok = True)

    geo_dict = {'state':  ['state_fips', 'state_codes', 'state_name'],
                'county': ['county_fips', 'county_name', 'state_codes', 'state_fips', 'state_name'],
                'cbsa':   ['cbsa_fips', 'cbsa_title'],
                'city':   ['city_geoid', 'city_name', 'city_state_code', 'city_population2020', 'city_population2022', 'city_rank', 'city_places_area_m2']}

    pooled_bins =   ["Total", "LincolnSale_BinsWide", "LincolnSale_BinsNarrow", "SaleAmt_PooledPercentile", "SaleAmt_PooledDecile", "SaleAmt_PooledQuintile"]
    snapshot_bins = ["Total", "LincolnSale_BinsWide", "LincolnSale_BinsNarrow", "SaleAmt_Percentile", "SaleAmt_Decile", "SaleAmt_Quintile"]

    def _percentile(self:pl.Expr, method='ordinal')-> pl.Expr:
        return self.rank(descending=True, method=method) / self.count()
    pl.Expr.percentile = _percentile

    logging.info('Snapshot collect -- memory usage: ' + mem_profile())
    if not (Path(output_dir) / 'sales_taxes_snapshot.parquet').exists():
        sales_unified_snapshot = (pl.scan_parquet(Path(output_dir) / 'sales_snapshot_staging.parquet', low_memory = True)
                        .filter(pl.col("PropertyClassID") == 'R')
                        .filter((pl.col("TaxAmt").is_not_null()) & (pl.col("SaleAmt").is_not_null()))
                        .with_columns([
                            # (pl.when((pl.col("TaxAmt").is_not_null()) & (pl.col("SaleAmt").is_not_null()) ).then(1).otherwise(0).alias("Tax_Sale_NotNull")),
                            (pl.col('TaxAmt') / pl.col('SaleAmt')).alias('TaxRate')
                        ])
                        .with_columns([
                            ((pl.col('SaleAmt').percentile().over(['SaleRecordingYear', 'FIPS'])) * 100).ceil().alias("SaleAmt_Percentile"),
                            ((pl.col('TaxRate').percentile().over(['SaleRecordingYear', 'FIPS'])) * 100).ceil().alias("TaxRate_Percentile")
                        ])
                        .with_columns([
                            (pl.col("SaleAmt_Percentile") / 10).ceil().alias("SaleAmt_Decile"),
                            (pl.col("TaxRate_Percentile") / 10).ceil().alias("TaxRate_Decile"),
                            (pl.col("SaleAmt_Percentile") / 20).ceil().alias("SaleAmt_Quintile"),
                            (pl.col("TaxRate_Percentile") / 20).ceil().alias("TaxRate_Quintile")
                        ])
                        .with_columns([
                            (pl.when((pl.col('SaleAmt') < 100000)).then(pl.lit("1 - <$100K"))
                                .when((pl.col('SaleAmt') >= 100000) & (pl.col('SaleAmt') <= 200000)).then(pl.lit("2 - $100K-$200K"))
                                .when((pl.col('SaleAmt') > 200000) & (pl.col('SaleAmt') < 250000)).then(pl.lit("3 - $200K-$250K"))
                                .when((pl.col('SaleAmt') >= 250000) & (pl.col('SaleAmt') <= 350000)).then(pl.lit("4 - $250K-$350K"))
                                .when((pl.col('SaleAmt') > 350000) & (pl.col('SaleAmt') < 1000000)).then(pl.lit("5 - $350K-$1M"))
                                .when((pl.col('SaleAmt') >= 1000000)).then(pl.lit("6 - >$1M"))
                                .otherwise(None).alias("LincolnSale_BinsWide")),
                            (pl.when((pl.col('SaleAmt') < 120000)).then(pl.lit("1 - <$120K"))
                                .when((pl.col('SaleAmt') >= 120000) & (pl.col('SaleAmt') <= 180000)).then(pl.lit("2 - $120K-$180K"))
                                .when((pl.col('SaleAmt') > 180000) & (pl.col('SaleAmt') < 270000)).then(pl.lit("3 - $180K-$270K"))
                                .when((pl.col('SaleAmt') >= 270000) & (pl.col('SaleAmt') <= 330000)).then(pl.lit("4 - $270K-$330K"))
                                .when((pl.col('SaleAmt') > 330000) & (pl.col('SaleAmt') < 1000000)).then(pl.lit("5 - $330K-$1M"))
                                .when((pl.col('SaleAmt') >= 1000000)).then(pl.lit("6 - >$1M"))
                                .otherwise(None).alias("LincolnSale_BinsNarrow")),
                            (pl.lit("Total")).alias("Total"),
                        ])
                        .with_columns([ 
                            (pl.when((pl.col("SaleAmt_Percentile") <= 2) | (pl.col("SaleAmt_Percentile") >= 99)).then(pl.lit('Tails')).otherwise(pl.lit('Middle 96%'))).alias("Sale_Outliers"),
                            (pl.when((pl.col("TaxRate_Percentile") <= 2) | (pl.col("TaxRate_Percentile") >= 99)).then(pl.lit('Tails')).otherwise(pl.lit('Middle 96%'))).alias("TaxRate_Outliers")
                        ])
                        ).collect(streaming=True)

        logging.info('Writing snapshot -- ' + mem_profile())
        sales_unified_snapshot.write_parquet(file = Path(output_dir) / 'sales_taxes_snapshot.parquet', use_pyarrow=True, compression="snappy")

    for geo_group, geo_list in geo_dict.items():
        logging.info(f"Aggregating snapshot: {geo_group}")
        print(geo_group)
        print(geo_list[0])
        for s_bin in snapshot_bins:
            print(s_bin)
            snapshot_agg = (pl.scan_parquet(Path(output_dir) / 'sales_taxes_snapshot.parquet', low_memory = True)
                        .filter( (pl.col("TaxRate_Outliers") == 'Middle 96%'))
                        #   (pl.col("SaleRecordingYear") == 2021) # & (pl.col("Tax_Sale_NotNull") == 1)
                        # (pl.col('TaxYearMatchSaleRecordingYear') == 1)  &
                        .group_by(geo_list + [s_bin] + ["SaleRecordingYear"]) 
                        .agg([
                            pl.count(),
                            pl.col('PropertyID').n_unique().alias('PropertyID_nunique'),
                            pl.col('TaxAmt').is_not_null().count().alias('TaxAmt_notnull'),
                            pl.col('SaleAmt').is_not_null().count().alias('SaleAmt_notnull'),
                            pl.col('TaxAmt').sum().alias('TaxAmt_sum'),
                            pl.col('SaleAmt').sum().alias('SaleAmt_sum'),
                            pl.col('TaxAmt').mean().alias('TaxAmt_mean'),
                            pl.col('SaleAmt').mean().alias('SaleAmt_mean'),
                            pl.col('TaxRate').mean().alias('TaxRate_mean'),
                            pl.col('TaxAmt').median().alias('TaxAmt_median'),
                            pl.col('SaleAmt').median().alias('SaleAmt_median'),
                            pl.col('TaxRate').median().alias('TaxRate_median'),
                            pl.col('TaxAmt').std().alias('TaxAmt_std'),
                            pl.col('SaleAmt').std().alias('SaleAmt_std'),
                            pl.col('TaxRate').std().alias('TaxRate_std')
                            ])
                        .with_columns([
                            (pl.col('TaxAmt_sum') / pl.col('SaleAmt_sum')).alias('TaxRate'),
                        ])
                        .with_columns([
                            (pl.col('SaleAmt_sum').over(geo_list).sum().alias('SaleAmt_Total')),
                            (pl.col('count').over(geo_list).sum().alias('count_Total'))
                        ])
                        .sort('count', geo_list[0], s_bin, descending=[True, False, True])
            ).collect(streaming=True)
            snapshot_agg.write_parquet(Path(output_dir) / 'agg_snapshot' / f'{geo_list[0]}_{s_bin}_snapshot.parquet', use_pyarrow=True, compression="snappy")

    logging.info('Pooled collect -- memory usage: ' + mem_profile())
    if not (Path(output_dir) / 'sales_taxes_pooled.parquet').exists():
        sales_unified_pooled = (pl.scan_parquet(Path(output_dir) / 'sales_pooled_staging.parquet', low_memory = True)
                        .filter(pl.col("PropertyClassID") == 'R')
                        .filter((pl.col("TaxAmt").is_not_null()) & (pl.col("SaleAmt").is_not_null()))
                        .filter((pl.col('MostRecentSale') == 1))
                        .with_columns([
                            (pl.col('SaleAmt') * (pl.col('hpi_index_2021'))).alias("SaleAmt_HPI2021"),
                            (pl.col('TaxAmt') * (pl.col('hpi_index_2021'))).alias("TaxAmt_Pooled_HPI2021")
                        ])
                        .with_columns([
                            # (pl.when((pl.col("TaxAmt_Pooled_HPI2021").is_not_null()) & (pl.col("SaleAmt_HPI2021").is_not_null())).then(1).otherwise(0).alias("TaxPooled_Sale_NotNull")),
                            (pl.col('TaxAmt_Pooled_HPI2021') / pl.col('SaleAmt_HPI2021')).alias('TaxRate_Pooled')
                        ])
                        .with_columns([
                            ((pl.col('SaleAmt_HPI2021').percentile().over(['SaleRecordingYear', 'FIPS', 'PropertyClassID'])) * 100).ceil().alias("SaleAmt_PooledPercentile"),
                            ((pl.col('TaxRate_Pooled').percentile().over(['SaleRecordingYear', 'FIPS', 'PropertyClassID'])) * 100).ceil().alias("TaxRate_PooledPercentile"),
                        ])
                        .with_columns([
                            (pl.col("SaleAmt_PooledPercentile") / 10).ceil().alias("SaleAmt_PooledDecile"),
                            (pl.col("TaxRate_PooledPercentile") / 10).ceil().alias("TaxRate_PooledDecile"),
                            (pl.col("SaleAmt_PooledPercentile") / 20).ceil().alias("SaleAmt_PooledQuintile"),
                            (pl.col("TaxRate_PooledPercentile") / 20).ceil().alias("TaxRate_PooledQuintile")
                        ])
                        .with_columns([
                            (pl.when((pl.col('SaleAmt_HPI2021') < 100000)).then(pl.lit("1 - <$100K"))
                                .when((pl.col('SaleAmt_HPI2021') >= 100000) & (pl.col('SaleAmt_HPI2021') <= 200000)).then(pl.lit("2 - $100K-$200K"))
                                .when((pl.col('SaleAmt_HPI2021') > 200000) & (pl.col('SaleAmt_HPI2021') < 250000)).then(pl.lit("3 - $200K-$250K"))
                                .when((pl.col('SaleAmt_HPI2021') >= 250000) & (pl.col('SaleAmt_HPI2021') <= 350000)).then(pl.lit("4 - $250K-$350K"))
                                .when((pl.col('SaleAmt_HPI2021') > 350000) & (pl.col('SaleAmt_HPI2021') < 1000000)).then(pl.lit("5 - $350K-$1M"))
                                .when((pl.col('SaleAmt_HPI2021') >= 1000000)).then(pl.lit("6 - >$1M"))
                                .otherwise(None).alias("LincolnSale_BinsWide")),
                            (pl.when((pl.col('SaleAmt_HPI2021') < 120000)).then(pl.lit("1 - <$120K"))
                                .when((pl.col('SaleAmt_HPI2021') >= 120000) & (pl.col('SaleAmt_HPI2021') <= 180000)).then(pl.lit("2 - $120K-$180K"))
                                .when((pl.col('SaleAmt_HPI2021') > 180000) & (pl.col('SaleAmt_HPI2021') < 270000)).then(pl.lit("3 - $180K-$270K"))
                                .when((pl.col('SaleAmt_HPI2021') >= 270000) & (pl.col('SaleAmt_HPI2021') <= 330000)).then(pl.lit("4 - $270K-$330K"))
                                .when((pl.col('SaleAmt_HPI2021') > 330000) & (pl.col('SaleAmt_HPI2021') < 1000000)).then(pl.lit("5 - $330K-$1M"))
                                .when((pl.col('SaleAmt_HPI2021') >= 1000000)).then(pl.lit("6 - >$1M"))
                                .otherwise(None).alias("LincolnSale_BinsNarrow")),
                            (pl.lit("Total")).alias("Total"),
                        ])
                        .with_columns([
                            (pl.when((pl.col("SaleAmt_PooledPercentile") <= 2) | (pl.col("SaleAmt_PooledPercentile") >= 99)).then(pl.lit('Tails')).otherwise(pl.lit('Middle 96%'))).alias("Sale_Pooled_Outliers"),
                            (pl.when((pl.col("TaxRate_PooledPercentile") <= 2) | (pl.col("TaxRate_PooledPercentile") >= 99)).then(pl.lit('Tails')).otherwise(pl.lit('Middle 96%'))).alias("TaxRate_Pooled_Outliers"),
                        ])
                        ).collect(streaming=True)

        logging.info('Writing pooled -- ' + mem_profile())
        sales_unified_pooled.write_parquet(file = Path(output_dir) / 'sales_taxes_pooled.parquet', use_pyarrow=True, compression="snappy")

    logging.info(f"Aggregations")
    logging.info('Memory usage: ' + mem_profile())
    for geo_group, geo_list in geo_dict.items():
        logging.info(f"Aggregating pooled: {geo_group}")
        print(geo_group)
        print(geo_list[0])
        for p_bin in pooled_bins:
            print(p_bin)
            pooled_agg = (pl.scan_parquet(Path(output_dir) / 'sales_taxes_pooled.parquet', low_memory = True)
                    .filter((pl.col("TaxRate_Pooled_Outliers") == 'Middle 96%') )
                    # & (pl.col("TaxPooled_Sale_NotNull") == 1) (pl.col('MostRecentSale') == 1) & 
                    .group_by(geo_list + [p_bin])
                    .agg([
                        pl.count(),
                        pl.col('PropertyID').n_unique().alias('PropertyID_nunique'),
                        pl.col('TaxAmt_Pooled_HPI2021').is_not_null().count().alias('TaxAmt_Pooled_HPI2021_notnull'),
                        pl.col('SaleAmt_HPI2021').is_not_null().count().alias('SaleAmt_HPI2021_notnull'),
                        pl.col('TaxAmt_Pooled_HPI2021').sum().alias('TaxAmt_Pooled_HPI2021_sum'),
                        pl.col('SaleAmt_HPI2021').sum().alias('SaleAmt_HPI2021_sum'),
                        pl.col('TaxAmt_Pooled_HPI2021').mean().alias('TaxAmt_Pooled_HPI2021_mean'),
                        pl.col('SaleAmt_HPI2021').mean().alias('SaleAmt_HPI2021_mean'),
                        pl.col('TaxRate_Pooled').mean().alias('TaxRate_Pooled_mean'),
                        pl.col('TaxAmt_Pooled_HPI2021').median().alias('TaxAmt_Pooled_HPI2021_median'),
                        pl.col('SaleAmt_HPI2021').median().alias('SaleAmt_HPI2021_median'),
                        pl.col('TaxRate_Pooled').median().alias('TaxRate_Pooled_median'),
                        pl.col('TaxAmt_Pooled_HPI2021').std().alias('TaxAmt_Pooled_HPI2021_std'),
                        pl.col('SaleAmt_HPI2021').std().alias('SaleAmt_HPI2021_std'),
                        pl.col('TaxRate_Pooled').std().alias('TaxRate_Pooled_std')
                        ])
                    .with_columns([
                        (pl.col('TaxAmt_Pooled_HPI2021_sum') / pl.col('SaleAmt_HPI2021_sum')).alias('TaxRate_Pooled'),
                        ])
                    .with_columns([
                        (pl.col('SaleAmt_HPI2021_sum').over(geo_list).sum().alias('SaleAmt_HPI2021_Total')),
                        (pl.col('count').over(geo_list).sum().alias('count_Total'))
                        ])
                    .sort('count', geo_list[0], p_bin, descending=[True, False, True])
            ).collect(streaming=True)
            pooled_agg.write_parquet(Path(output_dir) / 'agg_pooled' / f'{geo_list[0]}_{p_bin}_pooled.parquet', use_pyarrow=True, compression="snappy")

def setup(args=None):
    parser = argparse.ArgumentParser(description='Combine files.')
    parser.add_argument('--log_file', required=False, type=Path, dest="log_file", help="Path to write log file.")
    parser.add_argument('--input_dir', required=True, type=Path, dest="input_dir", help="Path to input directory.")
    parser.add_argument('--annual_file', required=True, type=str, dest="annual_file", help="Name of annual file.")
    parser.add_argument('--sale_file', required=True, type=str, dest="sale_file", help="Name of sale file.")
    parser.add_argument('--historical_file', required=True, type=str, dest="historical_file", help="Name of historical file.")
    parser.add_argument('--xwalk_dir', required=True, type=Path, dest="xwalk_dir", help="Path to crosswalk directory.")
    parser.add_argument('--xwalk_files', required=True, type=str, dest="xwalk_files", nargs='+', help="List of crosswalk file name.")
    parser.add_argument('--output_dir', required=True, type=Path, dest="output_dir", help="Path to output directory.")
    return parser.parse_args(args)

if __name__ == "__main__":
    main(**vars(setup()))

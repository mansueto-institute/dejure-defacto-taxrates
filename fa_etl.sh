

xwalk_list=(zcta.parquet block_groups_2020.parquet city_places.parquet congressional_districts_118.parquet elementary_school_districts.parquet secondary_school_districts.parquet unified_school_districts.parquet urban_classifications.parquet)

working_directory=/Users/nm/Downloads/fa_dev

log_file_arg=$working_directory/deployments/deploy_etl.log

input_dir_arg=$working_directory/inputs/raw
annual_file_arg=Prop01001.txt
sale_file_arg=Deed01001.txt
historical_file_arg=ValHist01001.txt

xwalk_dir_arg=$working_directory/inputs/xwalks
xwalk_files_arg=${xwalk_list[@]}

output_dir_arg=$working_directory/outputs

python fa-etl.py --log_file $log_file_arg --input_dir $input_dir_arg --annual_file $annual_file_arg --sale_file $sale_file_arg --historical_file $historical_file_arg --xwalk_dir $xwalk_dir_arg --xwalk_files $xwalk_files_arg --output_dir $output_dir_arg


# cd /project/crberry/first-american/code
# module load python/anaconda-2022.05
# conda activate propenv
# sbatch fa-etl.sbatch
# less /project/crberry/first-american/deployments/deploy_etl.err  
# less /project/crberry/first-american/deployments/deploy_etl.log
# tree /project/crberry/first-american/outputs
# squeue --user=nmarchio


rsync -avz nmarchio@midway.rcc.uchicago.edu:/project/crberry/first-american/outputs/crosswalk_dataset.parquet /Users/nm/Downloads


# scancel --user=nmarchio

# conda activate pinenv
# cd /Users/nm/Desktop/Projects/work/first-american/_dev
# bash fa_etl.sh


# cd /project/crberry/first-american/code
# rm fa-etl.py
# scp /Users/nm/Desktop/Projects/work/first-american/_dev/fa-etl.py nmarchio@midway.rcc.uchicago.edu:/project/crberry/first-american/code/

#scp /Users/nm/Desktop/Projects/work/first-american/_dev/fa-etl.py nmarchio@midway.rcc.uchicago.edu:/project/crberry/first-american/code/
#scp /Users/nm/Desktop/Projects/work/first-american/_dev/fa-etl.sbatch nmarchio@midway.rcc.uchicago.edu:/project/crberry/first-american/code/

# rsync -avz /Users/nm/Downloads/geos/xwalks nmarchio@midway.rcc.uchicago.edu:/project/crberry/first-american/inputs/xwalks

# assumes tracts_2010.parquet tracts_2020.parquet hpi_county.parquet county_cbsa_fips.parquet in xwalk dir

# module load python/anaconda-2022.05
# conda create --name propenv python=3.11.5 --yes
# source activate propenv
# conda install -c conda-forge polars --yes
# conda install -c conda-forge pandas --yes
# conda install -c conda-forge pyarrow --yes
# conda install -c conda-forge geopandas --yes
# conda install -c conda-forge urlpath --yes
# conda install -c conda-forge requests --yes
# conda install -c conda-forge dask --yes
# conda install -c conda-forge dask-geopandas --yes

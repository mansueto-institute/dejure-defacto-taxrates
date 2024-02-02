#!/bin/bash

#SBATCH --job-name=etl_job2
#SBATCH --partition=caslake
#SBATCH --nodes=1
#SBATCH --ntasks=32
#SBATCH --mem=175000
#SBATCH --output=/project/crberry/first-american/deployments/deploy_etl.out
#SBATCH --error=/project/crberry/first-american/deployments/deploy_etl.err
#SBATCH --mail-type=ALL
#SBATCH --mail-user=nmarchio@rcc.uchicago.edu
#SBATCH --time=36:00:00
#SBATCH --account=pi-crberry

module load python/anaconda-2022.05
conda activate propenv

# xwalk_list=(zcta.parquet block_groups_2020.parquet city_places.parquet congressional_districts_118.parquet elementary_school_districts.parquet secondary_school_districts.parquet unified_school_districts.parquet)
xwalk_list=(block_groups_2020.parquet city_places.parquet)

working_directory=/project/crberry/first-american

log_file_arg=$working_directory/deployments/deploy_etl.log

input_dir_arg=$working_directory/inputs
annual_file_arg=20220606_Annual.txt
sale_file_arg=20220606_DeedMtg.txt
historical_file_arg=20220606_ValueHistory.txt

xwalk_dir_arg=$working_directory/inputs/xwalks
xwalk_files_arg=${xwalk_list[@]}

output_dir_arg=$working_directory/outputs3

python fa-etl.py --log_file $log_file_arg --input_dir $input_dir_arg --annual_file $annual_file_arg --sale_file $sale_file_arg --historical_file $historical_file_arg --xwalk_dir $xwalk_dir_arg --xwalk_files $xwalk_files_arg --output_dir $output_dir_arg





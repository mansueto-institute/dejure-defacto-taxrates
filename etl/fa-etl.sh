
# /Users/nm/Desktop/fa-etl/fa-etl.sh
# xwalk_list=(zcta.parquet block_groups_2020.parquet city_places.parquet congressional_districts_118.parquet elementary_school_districts.parquet secondary_school_districts.parquet unified_school_districts.parquet)
xwalk_list=(block_groups_2020.parquet city_places.parquet)

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




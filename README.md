## First American Property Tax Rate Analysis

#### Build crosswalks
* Before processing the First American data it is necessary to generate geographic crosswalks and extract FHFA HPIs. To do so use the following script: `fa-xwalks.R`

#### Data processing
* To process the data deploy the script `fa-etl.py`. This was completed using the Midway3 High Performance Computer on a node with 175GM of memory using the following deployment configuration `fa-etl.sbatch`.

#### Assemble tables
* To assemble the tables in this [spreadsheet](https://docs.google.com/spreadsheets/d/1ipsXMEUDbFygD6tXtluf8t4aQ9LIXcqJBiTIc8DP9II/) use this script `fa-tables.R`.


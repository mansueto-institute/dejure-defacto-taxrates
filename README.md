## First American Property Tax Rate Analysis

#### Build crosswalks
* Before processing the First American data it is necessary to generate geographic crosswalks and extract FHFA HPIs. To do so use the following script: `etl/fa-xwalks.R` All crosswalks are in [this Box drive](https://uchicago.box.com/s/mmhsg7s9qs6jlov9u4kkt7vdoordt5kv).

#### Data processing
* To process the data deploy the script `etl/fa-etl.py`. This was completed using the Midway3 High Performance Computer on a node with 175GB of allocatable memory using the following deployment configuration `etl/fa-etl.sbatch`.

#### Assemble tables and charts
* To assemble the tables in this [spreadsheet](https://docs.google.com/spreadsheets/d/1mbOoV7PJ25wqBaafy8SGOIoci6q1BzzzHQ3IVV01e3A/edit#gid=1570744348) use this script `analysis/fa-analysis.R`. The data files and graphics for the analysis are located in [this Box drive](https://uchicago.box.com/s/9xiu62yt6nonbwazdmz9f5ju1xfui5uy).

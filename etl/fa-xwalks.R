
library(tidyverse)
library(sf)
library(dplyr)
library(ggplot2)
library(purrr)
library(tigris)
library(tidycensus)
library(stringr)
library(readxl)
library(viridis)
library(scales)
library(sfarrow)
library(arrow)

read_shp_zip <- function(zip_url) {
  tmp_filepath <- paste0(tempdir(), '/', basename(zip_url))
  download.file(url = paste0(zip_url), destfile = tmp_filepath)
  unzip(tmp_filepath, exdir=tempdir())
  data <- sf::st_read(gsub(".zip", ".shp", tmp_filepath))
  return(data)
}

# CBSA --------------------------------------------------------------------

xwalk_url <- 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls'
tmp_filepath <- paste0(tempdir(), '/', basename(xwalk_url))
download.file(url = paste0(xwalk_url), destfile = tmp_filepath)
cbsa_xwalk <- read_excel(tmp_filepath, sheet = 1, range = cell_rows(3:1919))
cbsa_xwalk <- cbsa_xwalk %>% 
  select_all(~gsub("\\s+|\\.|\\/", "_", .)) %>%
  rename_all(list(tolower)) %>%
  mutate(fips_state_code = str_pad(fips_state_code, width=2, side="left", pad="0"),
         fips_county_code = str_pad(fips_county_code, width=3, side="left", pad="0"),
         county_fips = paste0(fips_state_code,fips_county_code)) %>%
  rename(cbsa_fips = cbsa_code,
         area_type = metropolitan_micropolitan_statistical_area) %>%
  select(county_fips,cbsa_fips,cbsa_title,area_type,central_outlying_county) 

# write_parquet(cbsa_xwalk,  '/Users/nm/Downloads/geos/xwalks/cbsa_fips.parquet')

state_xwalk <- as.data.frame(fips_codes) %>%
  rename(state_fips = state_code,
         state_codes = state,
         county_name = county) %>%
  mutate(county_fips = paste0(state_fips,county_code))
state_fips <- unique(state_xwalk$state_fips)[1:51]
state_codes <- unique(state_xwalk$state_codes)[1:51]


acs_county <- get_acs(year = 2020, geography = "county", variables = 'B01003_001', 
                      geometry = FALSE, keep_geo_vars = FALSE, shift_geo = FALSE)

county_xwalk <- state_xwalk %>%
  left_join(., cbsa_xwalk, by = c('county_fips' = 'county_fips')) %>%
  mutate(cbsa_fips = case_when(is.na(cbsa_fips) ~ paste0('state_',state_fips), 
                               TRUE ~ cbsa_fips), 
         cbsa_title = case_when(is.na(cbsa_title) ~ paste0('Non-CBSA ', state_name), 
                                TRUE ~ cbsa_title), 
         area_type = case_when(is.na(area_type) ~ 'Non-CBSA',
                               area_type == "Metropolitan Statistical Area" ~ 'MSA',
                               area_type == "Micropolitan Statistical Area" ~ 'muSA',
                               TRUE ~ area_type), 
         central_outlying_county = case_when(is.na(central_outlying_county) ~ 'Residual', TRUE ~ central_outlying_county)) %>%
  left_join(., acs_county %>% select(GEOID, estimate) %>% rename(county_pop20 = estimate), by = c('county_fips'='GEOID')) %>%
  mutate(county_pop20 = replace_na(county_pop20, 0)) 
                                
# write_parquet(county_xwalk, '/Users/nm/Downloads/geos/xwalks/county_cbsa_fips.parquet')


# HPI ---------------------------------------------------------------------

hpi_metro <- read_csv('https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_metro.csv',
                col_names = c("metro", "fips", "year", "quarter", 'index', 'chng')) %>%
  mutate(index = case_when(index == '-' ~ NA,
                           TRUE ~ index))

hpi_url <- 'https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_nonmetro.xls'
tmp_filepath <- paste0(tempdir(), '/', basename(hpi_url))
download.file(url = paste0(hpi_url), destfile = tmp_filepath)
hpi_nonmetro <- read_xls(tmp_filepath, sheet ='HPI_AT_nonmetro', range = 'A3:E5408') %>%
  rename_all(list(tolower)) 

hpi_state <- read_csv('https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_state.csv',
                      col_names = c("state_code", "year", "quarter", 'index'))

# hpi_url <- 'https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_3zip.xlsx'
# tmp_filepath <- paste0(tempdir(), '/', basename(hpi_url))
# download.file(url = paste0(hpi_url), destfile = tmp_filepath)
# hpi_3zip <- read_xls(tmp_filepath,   sheet ='HPI_AT_3zip', range = 'A5:E101450') %>%
#   rename_all(list(tolower)) 

hpi_tract <- read_csv('https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_BDL_tract.csv')

hpi_url <- 'https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_BDL_county.xlsx'
tmp_filepath <- paste0(tempdir(), '/', basename(hpi_url))
download.file(url = paste0(hpi_url), destfile = tmp_filepath)
hpi_county <- read_xlsx(tmp_filepath, sheet = 'county', range = 'A7:H97668') %>%
  rename_all(list(tolower)) %>%
  select_all(~gsub("\\s+|\\.|\\/|%|\\(|\\)", "_", .)) 


county_xwalk <- county_xwalk %>%
  filter(!(state_fips  %in% c('60', '66', '69', '72', '74', '78')))

# cleanup
hpi_county_2 <- hpi_county %>% 
  select(fips_code, year, hpi) %>%  # state, county
  rename(county_fips = fips_code) %>% 
  mutate(hpi = case_when(hpi == '.' ~ NA_real_,
                         TRUE ~ as.numeric(hpi))) %>%
  mutate_at(vars(year), as.integer)  %>%
  filter(year >= 2000) %>%
  rename(hpi_county = hpi) %>%
  rbind(., county_xwalk %>% select(county_fips) %>%
          mutate(year = 2023, hpi_county = NA)) %>%
  complete(year, county_fips) %>%
  left_join(., county_xwalk %>% select(county_fips, state_fips, cbsa_fips), by = c('county_fips' = 'county_fips'))

hpi_metro_2 <- hpi_metro %>% 
  select(fips, year, quarter, index) %>% # metro
  rename(cbsa_fips = fips) %>%
  mutate_at(vars(index), as.numeric) %>% 
  group_by(cbsa_fips, year) %>%
  summarize_at(vars(index), list(mean), na.rm = TRUE) %>%
  ungroup() %>%
  filter(year >= 2000) %>%
  mutate_at(vars(cbsa_fips), as.character) %>%
  rename(hpi_metro = index)
  
hpi_nonmetro_2 <- hpi_nonmetro %>% 
  select(state, year, quarter, index)  %>% 
  group_by(state, year) %>%
  summarize_at(vars(index), list(mean), na.rm = TRUE) %>%
  ungroup() %>%
  left_join(., county_xwalk %>% select(state_codes, state_fips) %>% distinct(),
            by = c('state' = 'state_codes')) %>%
  #mutate(cbsa_fips = paste0('state_',state_fips)) %>%
  #select(cbsa_fips, year, index)  %>%
  select(state_fips, year, index)  %>%
  filter(year >= 2000) %>%
  rename(hpi_nonmetro = index)

hpi_state_2 <- hpi_state %>% 
  select(state_code, year, quarter, index) %>% 
  group_by(state_code, year) %>%
  summarize_at(vars(index), list(mean), na.rm = TRUE) %>%
  ungroup() %>%
  filter(year >= 2000) %>%
  left_join(., county_xwalk %>% select(state_codes, state_fips) %>% distinct(),
            by = c('state_code' = 'state_codes')) %>%
  rename(hpi_state = index) %>%
  select(state_fips, year,hpi_state )%>%
  complete(state_fips, year) 

hpi_county_combined <- hpi_county_2 %>%
  mutate(state_fips = str_sub(county_fips, 1, 2)) %>%
  left_join(., hpi_state_2, by = c('state_fips' = 'state_fips', 
                                   'year' = 'year')) %>%
  left_join(., hpi_nonmetro_2, by = c('state_fips' = 'state_fips', 
                                   'year' = 'year')) %>%
  left_join(., hpi_metro_2, by = c('cbsa_fips' = 'cbsa_fips' ,
                                  'year' = 'year')) %>%
  arrange(county_fips, year) %>%
  complete(year, nesting(county_fips, state_fips, cbsa_fips),
           fill = list(hpi_metro = NA, hpi_nonmetro = NA, hpi_state = NA, hpi_county = NA)) %>%
  mutate(hpi_metro_fill = hpi_metro,
         hpi_nonmetro_fill = hpi_nonmetro,
         hpi_state_fill = hpi_state,
         hpi_county_fill = hpi_county) %>%
  group_by(county_fips, state_fips, cbsa_fips) %>%
  fill(hpi_metro_fill, hpi_nonmetro_fill, hpi_state_fill, hpi_county_fill, .direction = "downup") %>%
  ungroup() 

hpi_county_imputed <- hpi_county_combined %>%
  select(year,county_fips,hpi_metro_fill,hpi_nonmetro_fill,hpi_state_fill,hpi_county_fill) %>%
  pivot_longer(cols = c("hpi_metro_fill","hpi_nonmetro_fill","hpi_state_fill","hpi_county_fill"),
               names_to = "type", values_to = "hpi") %>%
  filter(!is.na(hpi)) %>%
  arrange(type, county_fips, year) %>% 
  group_by(type, county_fips) %>%
  mutate(hpi_impute = stats::loess(hpi ~ as.numeric(year), span = 0.75, na.action = na.pass)$fitted) %>%
  ungroup() %>%
  select(year, county_fips, type, hpi_impute) %>%
  pivot_wider(names_from = type,
              names_glue = "{type}_{.value}",
              values_from = c(hpi_impute))
               
hpi_county_combined <- hpi_county_combined %>%
  left_join(., hpi_county_imputed, by = c('county_fips' = 'county_fips',
                                          'year' = 'year')) %>%
  mutate(hpi_county = coalesce(hpi_county, hpi_county_fill_hpi_impute),
         hpi_state = coalesce(hpi_state, hpi_state_fill_hpi_impute),
         hpi_nonmetro = coalesce(hpi_nonmetro, hpi_nonmetro_fill_hpi_impute),
         hpi_metro = coalesce(hpi_metro, hpi_metro_fill_hpi_impute)) %>%
  select(year, county_fips, state_fips, cbsa_fips, hpi_county, hpi_state, hpi_nonmetro, hpi_metro) %>%
  mutate(hpi_index = coalesce(hpi_county, hpi_metro,hpi_nonmetro, hpi_state))

hpi_county_final <- hpi_county_combined %>%
  left_join(., hpi_county_combined %>% filter(year == 2023) %>%
              select(county_fips, hpi_index) %>% rename(hpi_base_2023 = hpi_index),
            by = c('county_fips' = 'county_fips')) %>%
  left_join(., hpi_county_combined %>% filter(year == 2021) %>%
              select(county_fips, hpi_index) %>% rename(hpi_base_2021 = hpi_index),
            by = c('county_fips' = 'county_fips')) %>%
  mutate(hpi_index_2023 = hpi_base_2023/hpi_index,
         hpi_index_2021 = hpi_base_2021/hpi_index) %>%
  select(county_fips, year, hpi_index_2023, hpi_index_2021)
  
if (length(unique(hpi_county_final$county_fips))*length(unique(hpi_county_final$year))==nrow(hpi_county_final)) {
  print('balanced') 
} else {
  print('not balanced')
}

write_parquet(hpi_county_final, '/Users/nm/Downloads/geos/xwalks/county_hpi.parquet')

# -------------------------------------------------------------------------

# Read in Census Places (cities) population data
places_pop <- read_csv('https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/cities/totals/sub-est2022.csv') %>%
  rename_all(tolower) %>% 
  filter(sumlev %in% c('162')) %>%
  select(state, place, name, sumlev, stname, popestimate2020, popestimate2021, popestimate2022 ) %>%
  mutate(state = str_pad(state, width=2, side="left", pad="0"),
         place = str_pad(place, width=5, side="left", pad="0"),
         placeid = paste0(state,place)) %>%
  rename(cityname = name) %>%
  mutate(city_rank = row_number(desc(popestimate2022))) %>%
  rename(state_fips_city = state)

places <- places(cb = TRUE, year = 2020) %>%
  st_transform(4326) %>%
  left_join(., places_pop, by = c('GEOID'='placeid')) %>%
  rename_all(tolower) %>%
  select(geoid, name, stusps, popestimate2020, popestimate2022, city_rank, geometry) %>% 
  separate(data = ., col = 'name', into = 'name_short', sep =  "(\\[|-|]|/|[(])", remove = TRUE, extra = "drop") %>%
  rename(city_geoid = geoid, 
         city_name = name_short, 
         city_state_code = stusps, 
         city_population2020 = popestimate2020, 
         city_population2022 = popestimate2022) %>%
  st_make_valid() %>%
  filter(city_state_code %in% state_codes )

st_write_parquet(obj = places, dsn = '/Users/nm/Downloads/geos/city_places.parquet')
st_write(obj = places, dsn = '/Users/nm/Downloads/geos/city_places.geojson', delete_dsn = TRUE)
rm(places, places_pop)


schools <- school_districts(cb = TRUE, year = 2022) %>% 
  st_transform(4326) %>%
  rename_all(tolower) %>%
  select(geoid, stusps, name, higrade, lograde, geometry) %>%
  rename(school_geoid = geoid,
         school_state_code = stusps,
         school_name = name, 
         school_higrade = higrade, 
         school_lograde = lograde) %>%
  st_make_valid()

st_write_parquet(obj = schools, dsn = '/Users/nm/Downloads/geos/unified_school_districts.parquet')
st_write(obj = schools, dsn = '/Users/nm/Downloads/geos/unified_school_districts.geojson', delete_dsn = TRUE)
rm(schools)


scsd <- read_shp_zip('https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_scsd_500k.zip') %>%
  #st_read('/Users/nm/Downloads/cb_2022_us_scsd_500k/cb_2022_us_scsd_500k.shp')%>% 
  st_transform(4326) %>%
  rename_all(tolower) %>% st_make_valid() %>%
  select(geoid, name, higrade, lograde, stusps) %>%
  rename(hs_geoid = geoid, 
         hs_name = name,
         hs_higrade = higrade,
         hs_lograde = lograde,
         hs_statecode = stusps)

st_write_parquet(obj = scsd, dsn = '/Users/nm/Downloads/geos/secondary_school_districts.parquet')
st_write(obj = scsd, dsn = '/Users/nm/Downloads/geos/secondary_school_districts.geojson', delete_dsn = TRUE)
rm(scsd)
  
elsd <- read_shp_zip('https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_elsd_500k.zip') %>%
#st_read('/Users/nm/Downloads/cb_2022_us_elsd_500k/cb_2022_us_elsd_500k.shp') %>% 
  st_transform(4326) %>%
  rename_all(tolower) %>% st_make_valid() %>%
  select(geoid, name, higrade, lograde, stusps) %>%
  rename(elem_geoid = geoid,
         elem_name = name,
         elem_higrade = higrade,
         elem_lograde = lograde,
         elem_statecode = stusps)

st_write_parquet(obj = elsd, dsn = '/Users/nm/Downloads/geos/elementary_school_districts.parquet')
st_write(obj = elsd, dsn = '/Users/nm/Downloads/geos/elementary_school_districts.geojson', delete_dsn = TRUE)
rm(elsd)

cd118 <- read_shp_zip('https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_cd118_500k.zip') %>%
  st_transform(4326) %>% #st_read('/Users/nm/Downloads/cb_2022_us_cd118_500k/cb_2022_us_cd118_500k.shp')%>%
  rename_all(tolower) %>% st_make_valid() %>%
  select(geoid, cd118fp, statefp) %>%
  rename(cd118_geoid = geoid,
         cd118_distno = cd118fp, 
         cd118_statefips = statefp)

st_write_parquet(obj = cd118, dsn = '/Users/nm/Downloads/geos/congressional_districts_118.parquet')
st_write(obj = cd118, dsn = '/Users/nm/Downloads/geos/congressional_districts_118.geojson', delete_dsn = TRUE)
rm(cd118)

#file <- st_read('https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/Urbanization_Perceptions_Small_Area_Index/FeatureServer/28/query?outFields=*&where=1%3D1&f=geojson')
urban_class <- st_read('/Users/nm/Downloads/geos/Urbanization_Perceptions_Small_Area_Index.geojson') %>%
  rename_all(tolower)  %>%
  st_make_valid() %>%
  select(geoid, upsai_urban, upsai_suburban, upsai_rural, upsai_cat_controlled) %>%
  rename(tract_fips = geoid, 
         prob_urban = upsai_urban,
         prob_suburban = upsai_suburban,
         prob_rural = upsai_rural,
         prob_class_1u2s3r = upsai_cat_controlled)

st_write_parquet(obj = urban_class, dsn = '/Users/nm/Downloads/geos/urban_classifications.parquet')
st_write(obj = urban_class, dsn = '/Users/nm/Downloads/geos/urban_classifications.geojson', delete_dsn = TRUE)
rm(urban_class)


# BG ----------------------------------------------------------------------

block_groups <- read_shp_zip('https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_bg_500k.zip') %>%
  st_transform(4326) %>%
  rename_all(tolower) %>% st_make_valid() 

bg_lat_lon <- block_groups %>% st_centroid(.) %>%
  mutate(lon = map_dbl(geometry, ~st_point_on_surface(.x)[[1]]) ,
         lat = map_dbl(geometry, ~st_point_on_surface(.x)[[2]]) ) %>% 
  st_drop_geometry() %>%
  select(geoid, lon, lat)


block_groups <- block_groups %>%
  select(geoid, geometry) %>%
  rename(block_group_fips = geoid) %>%
  left_join(., bg_lat_lon, by = c('block_group_fips' = 'geoid'))

st_write_parquet(obj = block_groups , dsn = '/Users/nm/Downloads/geos/block_groups.parquet')
st_write(obj = block_groups , dsn = '/Users/nm/Downloads/geos/block_groups.geojson', delete_dsn = TRUE)
rm(block_groups )


# Tract -------------------------------------------------------------------

# Download state codes via tidycensus' "fips_codes" data set
state_xwalk <- as.data.frame(fips_codes) %>%
  rename(state_fips = state_code,
         state_codes = state,
         county_name = county) %>%
  mutate(county_fips = paste0(state_fips,county_code))
# Make lists for FIPS and codes
state_fips <- unique(state_xwalk$state_fips)[1:51]
state_codes <- unique(state_xwalk$state_codes)[1:51]


tract_all_us_data_2010 <- map_df(state_fips, function(x) {
  tigris::tracts(year = 2010, state = x)
})

tract_all_us_data_2010  <- tract_all_us_data_2010 %>%
  st_transform(4326) %>%
  rename_all(tolower) %>% st_make_valid()

tract_all_us_data_2010_lat_lon <- tract_all_us_data_2010 %>% st_centroid(.) %>%
  mutate(lon = map_dbl(geometry, ~st_point_on_surface(.x)[[1]]) ,
         lat = map_dbl(geometry, ~st_point_on_surface(.x)[[2]]) ) %>% 
  st_drop_geometry() %>%
  select(geoid10, lon, lat)

tract_all_us_data_2010  <- tract_all_us_data_2010  %>%
  select(geoid10, geometry) %>%
  rename(tract_fips_2010 = geoid10) %>%
  left_join(., tract_all_us_data_2010_lat_lon , by = c('tract_fips_2010' = 'geoid10'))

tract_all_us_data_2010  <- tract_all_us_data_2010  %>% 
  rename(lon_10 = lon,
         lat_10 = lat)

tract_all_us_data_2010  <- tract_all_us_data_2010  %>% 
  st_drop_geometry()

arrow:write_parquet(x = tract_all_us_data_2010, sink = '/Users/nm/Downloads/geos/xwalks/tracts_2010.parquet')
write_csv(z, '/Users/nm/Downloads/geos/xwalks/tracts_2010.csv')


# Use purrr function map_df to run a get_acs call that loops over all states
tract_all_us_data <- map_df(state_fips, function(x) {
  tigris::tracts(year = 2022, state = x)
})

tracts <- tract_all_us_data %>%
    st_transform(4326) %>%
    rename_all(tolower) %>% st_make_valid()

tracts <- read_shp_zip('https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_tract_500k.zip') %>%
  st_transform(4326) %>%
  rename_all(tolower) %>% st_make_valid()

t_lat_lon <- tracts %>% st_centroid(.) %>%
  mutate(lon = map_dbl(geometry, ~st_point_on_surface(.x)[[1]]) ,
         lat = map_dbl(geometry, ~st_point_on_surface(.x)[[2]]) ) %>% 
  st_drop_geometry() %>%
  select(geoid, lon, lat)

tracts <- tracts %>%
  select(geoid, geometry) %>%
  rename(tract_fips = geoid) %>%
  left_join(., t_lat_lon, by = c('tract_fips' = 'geoid'))

st_write_parquet(obj = tracts , dsn = '/Users/nm/Downloads/geos/tracts.parquet')
st_write(obj = tracts , dsn = '/Users/nm/Downloads/geos/tracts2.geojson', delete_dsn = TRUE)
rm(tracts)

# -------------------------------------------------------------------------

zcta <- read_shp_zip('https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_500k.zip') %>%
  st_transform(4326) %>%
  rename_all(tolower) %>% 
  st_make_valid() %>%
  rename(zipcode_zcta = geoid20) %>% 
  select(zipcode_zcta) 

st_write_parquet(obj = zcta, dsn = '/Users/nm/Downloads/geos/zcta.parquet')
st_write(obj = zcta, dsn = '/Users/nm/Downloads/geos/zcta.geojson', delete_dsn = TRUE)
rm(zcta)



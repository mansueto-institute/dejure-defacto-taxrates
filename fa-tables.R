
# -------------------------------------------------------------------------

library(tidyverse)
library(viridis)
library(arrow)
library(duckdb)
library(patchwork)
library(scales)
library(tidycensus)
library(viridis)
library(writexl)

pool <- read_parquet('/Users/nm/Downloads/results-fa2/sales_taxes_pooled.parquet')
snap <- read_parquet('/Users/nm/Downloads/results-fa2/sales_taxes_snapshot.parquet')

#z <- read_parquet('/Users/nm/Downloads/geos/xwalks/county_cbsa_fips.parquet')
#z <- read_parquet('/Users/nm/Downloads/geos/xwalks/city_places.parquet')

geo_vars = c("city_geoid", "city_name", "city_state_code", "city_population2020", "city_population2022", "city_rank" )
data_vars_snap = c("count", "TaxAmt_mean", "SaleAmt_mean", "TaxRate_mean", "TaxAmt_median", "SaleAmt_median", "TaxRate_median", "TaxRate")
data_vars_pool = c("count", "TaxAmt_Pooled_HPI2021_mean", "SaleAmt_HPI2021_mean", "TaxRate_Pooled_mean", "TaxAmt_Pooled_HPI2021_median", "SaleAmt_HPI2021_median", "TaxRate_Pooled_median", "TaxRate_Pooled")

# -------------------------------------------------------------------------

snap <- read_parquet('/Users/nm/Downloads/results-fa2/agg_snapshot/city_geoid_LincolnSale_BinsNarrow_snapshot.parquet')
pool <- read_parquet('/Users/nm/Downloads/results-fa2/agg_pooled/city_geoid_LincolnSale_BinsNarrow_pooled.parquet')

snap1_n <- snap %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  filter(LincolnSale_BinsNarrow %in% c('2 - $120K-$180K', '4 - $270K-$330K')) %>%
  arrange(city_geoid, LincolnSale_BinsNarrow) %>%
  mutate(LincolnSale_BinsNarrow = case_when(LincolnSale_BinsNarrow == '2 - $120K-$180K' ~ '120K_180K', 
                                            LincolnSale_BinsNarrow == '4 - $270K-$330K' ~ '270K_330K')) %>%
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid, LincolnSale_BinsNarrow) %>%
  mutate(rank_count_sum = row_number(desc(count_sum))) %>%
  ungroup() %>%
  filter(rank_count_sum  == 1) %>%
  select(all_of(c(geo_vars, 'SaleRecordingYear', 'LincolnSale_BinsNarrow', data_vars_snap ))) %>%
  mutate(LincolnSale_BinsNarrow = factor(LincolnSale_BinsNarrow, levels = c('120K_180K', '270K_330K'))) %>%
  pivot_wider(id_cols = all_of(c(geo_vars, 'SaleRecordingYear')),
              names_from = c(LincolnSale_BinsNarrow), 
              values_from = all_of(data_vars_snap) ) %>%
  arrange(desc(city_population2022)) %>%
  mutate_at(vars(city_geoid), as.integer)

pool1_n <- pool %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  filter(LincolnSale_BinsNarrow %in% c('2 - $120K-$180K', '4 - $270K-$330K')) %>%
  arrange(city_geoid, LincolnSale_BinsNarrow) %>%
  mutate(LincolnSale_BinsNarrow = case_when(LincolnSale_BinsNarrow == '2 - $120K-$180K' ~ '120K_180K', 
                                            LincolnSale_BinsNarrow == '4 - $270K-$330K' ~ '270K_330K')) %>%
  select(all_of(c(geo_vars,'LincolnSale_BinsNarrow',data_vars_pool ))) %>%
  mutate(LincolnSale_BinsWide = factor(LincolnSale_BinsNarrow, levels = c('120K_180K', '270K_330K'))) %>%
  pivot_wider(id_cols = all_of(geo_vars),
              names_from = c(LincolnSale_BinsNarrow), 
              values_from = all_of(data_vars_pool) ) %>%
  arrange(desc(city_population2022)) %>%
  mutate_at(vars(city_geoid), as.integer)


# -------------------------------------------------------------------------

snap <- read_parquet('/Users/nm/Downloads/results-fa2/agg_snapshot/city_geoid_LincolnSale_BinsWide_snapshot.parquet')
pool <- read_parquet('/Users/nm/Downloads/results-fa2/agg_pooled/city_geoid_LincolnSale_BinsWide_pooled.parquet')

snap1_w <- snap %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  filter(LincolnSale_BinsWide %in% c('2 - $100K-$200K', '4 - $250K-$350K')) %>%
  arrange(city_geoid, LincolnSale_BinsWide) %>%
  mutate(LincolnSale_BinsWide = case_when(LincolnSale_BinsWide == '2 - $100K-$200K' ~ '100K_200K', 
                                          LincolnSale_BinsWide == '4 - $250K-$350K' ~ '250K_350K')) %>%
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid, LincolnSale_BinsWide) %>%
  mutate(rank_count_sum = row_number(desc(count_sum))) %>%
  ungroup() %>%
  filter(rank_count_sum  == 1) %>%
  select(all_of(c(geo_vars, 'SaleRecordingYear', 'LincolnSale_BinsWide', data_vars_snap ))) %>%
  mutate(LincolnSale_BinsWide = factor(LincolnSale_BinsWide, levels = c('100K_200K','250K_350K'))) %>%
  pivot_wider(id_cols = all_of(c(geo_vars, 'SaleRecordingYear')),
              names_from = c(LincolnSale_BinsWide), 
              values_from = all_of(data_vars_snap) ) %>%
  arrange(desc(city_population2022)) %>%
  mutate_at(vars(city_geoid), as.integer)


pool1_w <- pool %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  filter(LincolnSale_BinsWide %in% c('2 - $100K-$200K', '4 - $250K-$350K')) %>%
  arrange(city_geoid, LincolnSale_BinsWide) %>%
  mutate(LincolnSale_BinsWide = case_when(LincolnSale_BinsWide == '2 - $100K-$200K' ~ '100K_200K', 
                                          LincolnSale_BinsWide == '4 - $250K-$350K' ~ '250K_350K')) %>%
  select(all_of(c(geo_vars,'LincolnSale_BinsWide',data_vars_pool ))) %>%
  mutate(LincolnSale_BinsWide = factor(LincolnSale_BinsWide, levels = c('100K_200K','250K_350K'))) %>%
  pivot_wider(id_cols = all_of(geo_vars),
              names_from = c(LincolnSale_BinsWide), 
              values_from = all_of(data_vars_pool) ) %>%
  arrange(desc(city_population2022)) %>%
  mutate_at(vars(city_geoid), as.integer)



# -------------------------------------------------------------------------

snap <- read_parquet('/Users/nm/Downloads/results-fa2/agg_snapshot/city_geoid_SaleBins_snapshot.parquet')
pool <- read_parquet('/Users/nm/Downloads/results-fa2/agg_pooled/city_geoid_SaleBins_pooled.parquet')

snap1_b <- snap %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  arrange(city_geoid, SaleBins) %>%
  mutate(SaleBins = case_when(SaleBins == "1 - <$100K" ~ "under100K",
                              SaleBins == "2 - $100K-$200K" ~ "100K_200K",
                              SaleBins == "3 - $200K-$300K" ~ "200K_300K",
                              SaleBins == "4 - $300K-$400K" ~ "300K_400K",
                              SaleBins == "5 - $400K-$500K" ~ "400K_500K",
                              SaleBins == "6 - $500K-$600K" ~ "500K_600K",
                              SaleBins == "7 - $600K-$700K" ~ "600K_700K",
                              SaleBins == "8 - $700K-$800K" ~ "700K_800K",
                              SaleBins == "9 - $800K-$900K" ~ "800K_900K",
                              SaleBins == "10 - $900K-$1M" ~ "900K_1M",
                              SaleBins == "11 - >$1M" ~ "over1M")) %>%
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid, SaleBins) %>%
  mutate(rank_count_sum = row_number(desc(count_sum))) %>%
  ungroup() %>%
  filter(rank_count_sum  == 1) %>%
  select(all_of(c(geo_vars, 'SaleRecordingYear', 'SaleBins', data_vars_snap ))) %>%
  mutate(SaleBins = factor(SaleBins, levels = c("under100K", "100K_200K", "200K_300K", "300K_400K", "400K_500K", "500K_600K", "600K_700K", "700K_800K", "800K_900K", "900K_1M", "over1M"))) %>%
  pivot_wider(id_cols = all_of(c(geo_vars, 'SaleRecordingYear')),
              names_from = c(SaleBins), 
              values_from = all_of(data_vars_snap) ) %>%
  arrange(desc(city_population2022)) %>%
  mutate_at(vars(city_geoid), as.integer)


pool1_b <- pool %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  arrange(city_geoid, SaleBins) %>%
  mutate(SaleBins = case_when(SaleBins == "1 - <$100K" ~ "under100K",
                              SaleBins == "2 - $100K-$200K" ~ "100K_200K",
                              SaleBins == "3 - $200K-$300K" ~ "200K_300K",
                              SaleBins == "4 - $300K-$400K" ~ "300K_400K",
                              SaleBins == "5 - $400K-$500K" ~ "400K_500K",
                              SaleBins == "6 - $500K-$600K" ~ "500K_600K",
                              SaleBins == "7 - $600K-$700K" ~ "600K_700K",
                              SaleBins == "8 - $700K-$800K" ~ "700K_800K",
                              SaleBins == "9 - $800K-$900K" ~ "800K_900K",
                              SaleBins == "10 - $900K-$1M" ~ "900K_1M",
                              SaleBins == "11 - >$1M" ~ "over1M")) %>%
  select(all_of(c(geo_vars,'SaleBins',data_vars_pool ))) %>%
  mutate(SaleBins = factor(SaleBins, levels = c("under100K", "100K_200K", "200K_300K", "300K_400K", "400K_500K", "500K_600K", "600K_700K", "700K_800K", "800K_900K", "900K_1M", "over1M"))) %>%
  pivot_wider(id_cols = all_of(geo_vars),
              names_from = c(SaleBins), 
              values_from = all_of(data_vars_pool) ) %>%
  arrange(desc(city_population2022))  %>%
  mutate_at(vars(city_geoid), as.integer)


# -------------------------------------------------------------------------

snap <- read_parquet('/Users/nm/Downloads/results-fa2/agg_snapshot/city_geoid_SaleAmt_Quintile_snapshot.parquet')
pool <- read_parquet('/Users/nm/Downloads/results-fa2/agg_pooled/city_geoid_SaleAmt_PooledQuintile_pooled.parquet')

snap1_q <- snap %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid, SaleAmt_Quintile) %>%
  mutate(rank_count_sum = row_number(desc(count_sum))) %>%
  ungroup() %>%
  filter(rank_count_sum  == 1) %>%
  select(all_of(c(geo_vars, 'SaleRecordingYear', 'SaleAmt_Quintile',data_vars_snap ))) %>%
  mutate(SaleAmt_Quintile = factor(SaleAmt_Quintile, levels = c(1,2,3,4,5))) %>%
  arrange(city_geoid, SaleAmt_Quintile) %>%
  pivot_wider(id_cols = all_of(c(geo_vars, 'SaleRecordingYear')),
              names_from = c(SaleAmt_Quintile), 
              values_from = all_of(data_vars_snap))%>%
  arrange(desc(city_population2022))  %>%
  mutate_at(vars(city_geoid), as.integer)

pool1_q <- pool %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  select(all_of(c(geo_vars,'SaleAmt_PooledQuintile',data_vars_pool ))) %>%
  arrange(city_geoid, SaleAmt_PooledQuintile) %>%
  mutate(SaleAmt_Quintile = factor(SaleAmt_PooledQuintile, levels = c(1,2,3,4,5))) %>%
  pivot_wider(id_cols = all_of(geo_vars),
              names_from = c(SaleAmt_PooledQuintile), 
              values_from = all_of(data_vars_pool) )%>%
  arrange(desc(city_population2022))  %>%
  mutate_at(vars(city_geoid), as.integer)

# -------------------------------------------------------------------------

snap <- read_parquet('/Users/nm/Downloads/results-fa2/agg_snapshot/city_geoid_Total_snapshot.parquet')
pool <- read_parquet('/Users/nm/Downloads/results-fa2/agg_pooled/city_geoid_Total_pooled.parquet')

snap1_t <- snap %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid, Total) %>%
  mutate(rank_count_sum = row_number(desc(count_sum))) %>%
  ungroup() %>%
  filter(rank_count_sum  == 1) %>%
  select(all_of(c(geo_vars, 'SaleRecordingYear', 'Total',data_vars_snap ))) %>%
  arrange(city_geoid, Total) %>%
  arrange(desc(city_population2022))  %>%
  mutate_at(vars(city_geoid), as.integer)

pool1_t <- pool %>%
  filter(city_geoid %in% c('0107000', '0203000', '0455000', '0541000', '0644000', '0820000', '0908000', '0446000', '0455000', '0477000', '0603526', '0627000', '0643000', '0644000', '0653000', '0664000', '0666000', '0667000', '0668000', '0816000', '0820000', '1150000', '1077580', '1235000', '1304000', '1571550', '1608830', '1703012', '1714000', '1836003', '1921000', '2079000', '2148006', '2255000', '2360545', '2404000', '2507000', '2622000', '2743000', '2836000', '2938000', '3006550', '3137000', '3240000', '3345140', '3451000', '3502000', '3611000', '3651000', '3712000', '3825700', '3918000', '4055000', '4159000', '4260000', '4459000', '4513330', '4659020', '4752006', '4835000', '4967000', '5010675', '5182000', '5363000', '5414600', '5553000', '5613900', '1150000', '1235000', '1245000', '1304000', '1714000', '1836003', '2079000', '2148006', '2404000', '2507000', '2622000', '2743000', '2938000', '3137000', '3240000', '3502000', '3651000', '3712000', '3755000', '3918000', '4055000', '4075000', '4159000', '4260000', '4748000', '4752006', '4804000', '4805000', '4819000', '4824000', '4827000', '4835000', '4865000', '5182000', '5363000', '5553000')) %>%
  select(all_of(c(geo_vars,'Total',data_vars_pool ))) %>%
  arrange(city_geoid, Total) %>%
  arrange(desc(city_population2022)) %>%
  mutate_at(vars(city_geoid), as.integer)

# -------------------------------------------------------------------------


lincoln_tables <- read_csv('/Users/nm/Downloads/results-fa2/Lincoln-Institute-Tables - Lincoln2021.csv')

snap_all <- lincoln_tables %>%
  select(year, table_state, table_city, place_id, city_name, state_code, state_name, population_2020, tax_rate_median_value, tax_rate_median_value_assessment_limits, tax_rate_150k, tax_rate_300k) %>% 
  distinct() %>%
  left_join(., snap1_t %>% select(city_geoid, count, TaxRate_median), by = c('place_id'= 'city_geoid')) %>%
  left_join(., snap1_n %>% select(city_geoid, count_120K_180K, count_270K_330K, TaxRate_median_120K_180K, TaxRate_median_270K_330K), by = c('place_id'= 'city_geoid')) %>%
  left_join(., snap1_w %>% select(city_geoid, count_100K_200K, count_250K_350K, TaxRate_median_100K_200K, TaxRate_median_250K_350K), by = c('place_id'= 'city_geoid')) %>%
  left_join(., snap1_b %>% select(city_geoid, TaxRate_median_under100K, TaxRate_median_100K_200K, TaxRate_median_200K_300K, TaxRate_median_300K_400K, TaxRate_median_400K_500K, TaxRate_median_500K_600K, TaxRate_median_600K_700K, TaxRate_median_700K_800K, TaxRate_median_800K_900K, TaxRate_median_900K_1M, TaxRate_median_over1M), by = c('place_id'='city_geoid'))

pool_all <- lincoln_tables %>%
  select(year, table_state, table_city, place_id, city_name, state_code, state_name, population_2020, tax_rate_median_value, tax_rate_median_value_assessment_limits, tax_rate_150k, tax_rate_300k) %>% 
  distinct() %>%
  left_join(., pool1_t %>% select(city_geoid,  count,  TaxRate_Pooled_median), by = c('place_id'= 'city_geoid')) %>%
  left_join(., pool1_n %>% select(city_geoid, count_120K_180K, count_270K_330K, TaxRate_Pooled_median_120K_180K, TaxRate_Pooled_median_270K_330K), by = c('place_id'= 'city_geoid')) %>%
  left_join(., pool1_w %>% select(city_geoid, count_100K_200K, count_250K_350K, TaxRate_Pooled_median_100K_200K, TaxRate_Pooled_median_250K_350K), by = c('place_id'= 'city_geoid')) %>%
  left_join(., pool1_b %>% select(city_geoid, TaxRate_Pooled_median_under100K, TaxRate_Pooled_median_100K_200K, TaxRate_Pooled_median_200K_300K, TaxRate_Pooled_median_300K_400K, TaxRate_Pooled_median_400K_500K, TaxRate_Pooled_median_500K_600K, TaxRate_Pooled_median_600K_700K, TaxRate_Pooled_median_700K_800K, TaxRate_Pooled_median_800K_900K, TaxRate_Pooled_median_900K_1M, TaxRate_Pooled_median_over1M), by = c('place_id'= 'city_geoid')) 


write_xlsx(list('combined_single' = snap_all,
                'combined_pooled' = pool_all,
                'total_single' = snap1_t, 
                'total_pooled' = pool1_t,
                'narrow_bin_single' = snap1_n, 
                'narrow_bin_pooled' = pool1_n,
                'wide_bin_single' = snap1_w, 
                'wide_bin_pooled' = pool1_w,
                'quintile_bin_single' = snap1_q, 
                'quintile_bin_pooled' = pool1_q,
                'sale_bin_single' = snap1_b,
                'sale_bin_pooled' = pool1_b),
           col_names = TRUE, format_headers = TRUE, path = paste0('/Users/nm/Downloads/results-fa2/combined.xlsx'))


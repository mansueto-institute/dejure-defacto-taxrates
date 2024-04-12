
library(tidyverse)
library(arrow)
library(sf)
library(duckdb)
library(patchwork)
library(scales)
library(tidycensus)
library(viridis)
library(writexl)
library(tigris)

options(scipen=9999)

# Read in data ------------------------------------------------------------
# Data link: https://uchicago.box.com/s/9xiu62yt6nonbwazdmz9f5ju1xfui5uy
workdir = '/Users/nm/Downloads/dejure_defacto_data/'

city_vars <- c("city_geoid", "city_name", "city_state_code", "city_population2022", "city_rank", "city_places_area_m2")
lincoln_data <- read_csv(paste0(workdir,'Lincoln2021.csv'))
micro <- read_parquet(paste0(workdir,'sales_staging_tiles.parquet'))
s_quintile <- read_csv(paste0(workdir,'Snapshot_city_geoid_SaleAmt_Quintile_query.csv'))
s_total <- read_csv(paste0(workdir,'Snapshot_city_geoid_Total_query.csv'))

# -------------------------------------------------------------------------

micro_2 <- micro %>% 
  mutate(count = 1) %>%
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid) %>%
  mutate(count_rank = dense_rank(desc(count_sum))) %>%
  ungroup()%>%
  filter(count_rank == 1 & count_sum > 1) %>%
  filter(!is.na(TaxRate) & TaxRate > 0, # & TaxRate < .2,
         TaxRate_Outliers == 'Middle 96%',
         TaxRate <= .5,
         #Sale_Outliers == 'Middle 96%',
         !is.na(city_geoid),
         RecentSaleByYear == 1) %>%
  mutate(city_geoid_int = as.integer(city_geoid)) %>%
  filter( city_geoid_int %in% unique(lincoln_data$place_id) ) %>%
  select(PropertyID, SaleRecordingYear, city_geoid, city_name, city_state_code, count, TaxRate, TaxAmt, SaleAmt, LincolnSale_BinsNarrow, city_population2022) %>%
  mutate(city_state = paste0(city_name, ', ', city_state_code)) %>%
  group_by(city_state) %>%
  mutate(IQR_city = IQR(TaxRate)) %>%
  ungroup() %>%
  mutate(IQR_rank = dense_rank(IQR_city),
         IQR_quartile = case_when(IQR_rank <= 18 ~ 1,
                                  IQR_rank > 18 & IQR_rank <= 36 ~ 2,
                                  IQR_rank > 36 & IQR_rank <= 54 ~ 3,
                                  IQR_rank > 54 ~ 4)) %>%
  group_by(city_state) %>%
  mutate(sale_quintile = ntile(SaleAmt, 5)) %>%
  ungroup()

# IQR table ---------------------------------------------------------------

iqr_table <- micro_2 %>%
  group_by(city_geoid, city_name, city_state) %>%
  summarize(tax_rate_median = median(TaxRate), 
            tax_rate_mean = mean(TaxRate),
            tax_rate_sd = sd(TaxRate),
            tax_rate_10pct = quantile(TaxRate, probs = 0.10),
            tax_rate_25pct = quantile(TaxRate, probs = 0.25),
            tax_rate_50pct = quantile(TaxRate, probs = 0.50),
            tax_rate_75pct = quantile(TaxRate, probs = 0.75),
            tax_rate_90pct = quantile(TaxRate, probs = 0.90),
            iqr_diff75_25 = IQR(TaxRate),
            coeff_of_var = tax_rate_sd/tax_rate_mean) %>%
  ungroup() 

write_csv(iqr_table, paste0(workdir,'iqr_table.csv'))

# Box plots chart ---------------------------------------------------------------

(boxplot_chart <- ggplot(micro_2, aes(x = TaxRate, 
                           y = reorder(city_state, IQR_city) ) ) + 
    geom_boxplot(outlier.shape = NA) + 
    scale_x_continuous(limits = c(0,.08), expand = c(0,0), labels = percent_format() ) +
    labs(y = '', x = 'De Facto Tax Rate') + 
    #scale_fill_manual(values = c('#FF9671', '#D65DB1',  '#2C73D2',  '#00C9A7'), name = '') +
    theme_classic() +
    theme(legend.position = 'bottom'))

ggsave(plot = boxplot_chart, filename = paste0(workdir,'boxplot_chart.pdf'), width = 8, height = 10)
ggsave(plot = boxplot_chart, filename = paste0(workdir,'boxplot_chart.png'), dpi = 300, width = 8, height = 10)

# Table of de jure vs de facto table --------------------------------------

lincoln_data_subset <- lincoln_data %>%
  rename(lincoln_tax_rate_median = tax_rate_median_value,
         lincoln_tax_rate_150k = tax_rate_150k,
         lincoln_tax_rate_300k = tax_rate_300k) %>%
  select(place_id, lincoln_tax_rate_median, lincoln_tax_rate_150k, lincoln_tax_rate_300k) %>%
  group_by(place_id) %>%
  mutate(dup_flag = row_number()) %>%
  ungroup() %>%
  filter(dup_flag == 1)

micro_3_total <- micro_2 %>%
  group_by(city_geoid, city_name, city_state_code, SaleRecordingYear) %>%
  summarize(TaxRate_median = median(TaxRate, na.rm = TRUE),
            count = sum(count)
  ) %>%
  ungroup()

micro_3_narrow_bins <- micro_2 %>%
  group_by(city_geoid,  LincolnSale_BinsNarrow) %>% # city_name, city_state_code, SaleRecordingYear,
  summarize(TaxRate_median = median(TaxRate, na.rm = TRUE),
            count = sum(count)
  ) %>%
  ungroup() %>%
  filter(LincolnSale_BinsNarrow %in% c('2 - $120K-$180K', '4 - $270K-$330K')) %>%
  pivot_wider(id_cols = all_of(c('city_geoid')), # , 'city_name', 'SaleRecordingYear'
              names_from = c(LincolnSale_BinsNarrow), 
              values_from = all_of(c('count', 'TaxRate_median', 'count')))

table1 <- micro_3_total %>%
  left_join(., micro_3_narrow_bins, by = c('city_geoid' = 'city_geoid'))  %>% 
  mutate(city_geoid_int = as.integer(city_geoid)) %>%
  left_join(., lincoln_data_subset,
            by = c('city_geoid_int' = 'place_id') ) %>%
  mutate(city_state = paste0(city_name, ', ', city_state_code)) %>%
  left_join(., s_total_2 %>% select(city_geoid_int, TaxRate_median) %>% rename( TaxRate_median_py =  TaxRate_median),
            by = c('city_geoid_int'= 'city_geoid_int')
  ) %>%
  select(city_geoid, city_name, city_state_code, SaleRecordingYear, city_state, 
         TaxRate_median, `TaxRate_median_2 - $120K-$180K`, `TaxRate_median_4 - $270K-$330K`, 
         lincoln_tax_rate_median, lincoln_tax_rate_150k, lincoln_tax_rate_300k) 

write_csv(table1 , paste0(workdir,'dejure_defacto_table.csv'))


# Totals ------------------------------------------------------------------

s_total_2 <- s_total %>%
  mutate(city_geoid_int = as.integer(city_geoid)) %>%
  filter(city_geoid_int %in% unique(lincoln_data$place_id)) %>% 
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid) %>%
  mutate(rank_count_sum = row_number(desc(count_sum))) %>%
  ungroup() %>%
  filter(rank_count_sum  == 1) %>%
  mutate(city_state = paste0(city_name, ', ', city_state_code)) %>%
  mutate(city_geoid_int = as.integer(city_geoid)) %>%
  left_join(., lincoln_data %>%
              select(place_id, tax_rate_median_value) %>% distinct(),
            by = c('city_geoid_int' = 'place_id') ) 


write_csv(
  s_total_2 %>% select(city_geoid, city_name, city_state_code, city_state, city_population2020, city_population2022, city_rank, Total, SaleRecordingYear, count,
                       tax_rate_median_value, TaxRate, TaxRate_mean, TaxRate_median, TaxRate_moe, TaxRate_upper, TaxRate_lower, TaxRate_std) %>%
    mutate(TaxRate_coefficient_of_variation = TaxRate_std/TaxRate) %>%
    rename(TaxRate_median_LincolnInstitute = tax_rate_median_value)
  ,
  paste0(workdir,'totals_table.csv'))


# Quintiles ---------------------------------------------------------------

s_quintile_2 <- s_quintile %>%
  mutate(city_geoid_int = as.integer(city_geoid)) %>%
  filter(city_geoid_int %in% unique(lincoln_data$place_id)) %>% 
  group_by(city_geoid, SaleRecordingYear) %>%
  mutate(count_sum = sum(count)) %>%
  ungroup() %>%
  group_by(city_geoid, SaleAmt_Quintile) %>%
  mutate(rank_count_sum = row_number(desc(count_sum))) %>%
  ungroup() %>%
  filter(rank_count_sum  == 1) %>%
  mutate(city_state = paste0(city_name, ', ', city_state_code))

s_quintile_w <- s_quintile_2 %>%
  pivot_wider(id_cols = all_of(c(city_vars, 'city_state', 'SaleRecordingYear')),
              names_from = c(SaleAmt_Quintile), 
              values_from = all_of(c('count', 'SaleAmt_median', 'SaleAmt_mean', 'TaxRate', 'TaxRate_median', 'TaxRate_mean', 'TaxRate_std', 'AssdValue_SaleAmt_Ratio_median', 'TaxAmt_sum', 'SaleAmt_sum'))) %>%
  arrange(desc(city_population2022)) %>%
  # Two-Sample Z-test
  mutate(test_stat_mean = (TaxRate_mean_5 - TaxRate_mean_1) / sqrt( (TaxRate_std_5^2)/count_5 + (TaxRate_std_1^2)/count_1  ),
         gt_critval_mean = ifelse(test_stat_mean  > qnorm((1 + .90) / 2),1,0),
         TaxRate_5_1_diff_mean = (TaxRate_mean_5 - TaxRate_mean_1),
         
         test_stat_med = (TaxRate_median_5 - TaxRate_median_1) / sqrt( (TaxRate_std_5^2)/count_5 + (TaxRate_std_1^2)/count_1  ),
         gt_critval_med = ifelse(test_stat_med > qnorm((1 + .90) / 2),1,0),
         TaxRate_5_1_diff_med = (TaxRate_median_5 - TaxRate_median_1),
         
         ratio = TaxRate_median_5/TaxRate_1,
         ratio_fixed =  TaxRate_median_5/ TaxRate_median_1
  ) 

s_quintile_viz <- s_quintile_2 %>%
  left_join(., s_quintile_w %>% select(city_geoid, TaxRate_5_1_diff_mean, TaxRate_5_1_diff_med ),
            by = c('city_geoid'= 'city_geoid')) %>%
  mutate(city_geoid_int = as.integer(city_geoid)) %>%
  left_join(., lincoln_data %>%
              select(place_id, tax_rate_median_value) %>% distinct(), 
            by = c('city_geoid_int' = 'place_id') )

(quintile_chart <- ggplot(data = s_quintile_viz %>%  filter(SaleAmt_Quintile %in% c(1,5)), 
                          aes(x = reorder(city_state, TaxRate_5_1_diff_med), y =  TaxRate_median )) +
    #coord_flip() + 
    geom_point(show.legend=FALSE) + geom_line(show.legend=FALSE) + 
    geom_point(data = s_quintile_viz %>%  filter(SaleAmt_Quintile %in% c(1,5)), 
               aes(x = reorder(city_state, TaxRate_5_1_diff_med), 
                   y =  TaxRate_median, 
                   color = as.character(SaleAmt_Quintile)), size = 2, show.legend=TRUE ) + #3
    geom_point(data = s_quintile_viz %>% select(city_state, tax_rate_median_value) %>% distinct(),
               aes(y = tax_rate_median_value, x = city_state,
                   color = '#FF9671')) +
    labs(x = '', y = 'Effective tax rate (difference between top and bottom quintiles)') +
    scale_y_continuous(labels = percent_format()) +
    scale_color_manual(name="", 
                       values = c('#FF9671', 'black', '#00C9A7'),
                       labels=c('De Jure Tax Rate', 'Top quintile', 'Bottom quintile')) +
    theme_bw() + 
    theme(legend.position = 'bottom',
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)))

ggsave(plot = quintile_chart, filename = paste0(workdir,'quintile_chart.pdf'), width = 10, height = 8)
ggsave(plot = quintile_chart, filename = paste0(workdir,'quintile_chart.png'), dpi = 300, width = 10, height = 8)

write_csv(
  s_quintile_w %>% select(city_geoid, city_name, city_state_code, city_population2022, city_rank, city_state, SaleRecordingYear, 
                          TaxRate_median_1, TaxRate_median_2, TaxRate_median_3, TaxRate_median_4, TaxRate_median_5, 
                          ratio, ratio_fixed,
                          count_1, count_2, count_3, count_4, count_5
  ),
  paste0(workdir,'quintiles_table.csv')
)


# Confidence intervals ----------------------------------------------------

micro_3 <- micro_2 %>%
  group_by(city_geoid, city_name, city_state_code, SaleRecordingYear) %>%
  summarize(TaxRate_median_est = DescTools::MedianCI(x = TaxRate, conf.level = 0.90, sides = c("two.sided"), na.rm = TRUE)[[1]],
            TaxRate_median_lo_ci = DescTools::MedianCI(x = TaxRate, conf.level = 0.90, sides = c("two.sided"), na.rm = TRUE)[[2]],
            TaxRate_median_hi_ci = DescTools::MedianCI(x = TaxRate, conf.level = 0.90, sides = c("two.sided"), na.rm = TRUE)[[3]] #, method = "boot"
  ) %>%
  ungroup() %>%
  mutate(city_geoid_int = as.integer(city_geoid)) %>%
  left_join(., lincoln_data %>%
              select(place_id, tax_rate_median_value) %>% 
              rename(lincoln_tax_rate_median = tax_rate_median_value) %>% distinct(), 
            by = c('city_geoid_int' = 'place_id') ) %>%
  mutate(tax_rate_diff = TaxRate_median_est - lincoln_tax_rate_median ) %>%
  mutate(city_state = paste0(city_name, ', ', city_state_code)) %>%
  mutate(outside_ci = case_when( TaxRate_median_lo_ci >  lincoln_tax_rate_median ~ 'Outside CI',
                                 TaxRate_median_hi_ci <  lincoln_tax_rate_median ~ 'Outside CI',
                                 TRUE ~ 'Within CI'))



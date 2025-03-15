# SCORE-CVD: Myocardial infarction alogrithm 
# Prepare HES-APC
# 01_hes_apc_prepare.R
# BHF Data Science Centre, 2025
#
# Authors:
# - James Farrell, BHF Data Science Centre
# 
# Date Created: 2025-03-08
# Last updated: 2025-03-08
# Version:      v0.1
#
# This script loads the HES-APC dataset and performs the following:
# - Combines financial years if necessary
# - Assigning numeric, date and integer columns
# - Removing null dates
# - Rename columns for consistency
# - Removing bad episodes (with flowchart)
# - Saves cleaned dataset

library(tidyverse)
library(lubridate)

# Parameters
## Folder containing HES-APC data
hes_apc_folder = here::here("data/hes_apc")

## Column with person identifier (to be renamed person_id)
person_id_variable = "PSEUDO_HESID"

## Columns with non-character data types
numeric_cols = c("STARTAGE_CALC")
date_cols = c("ADMIDATE", "DISDATE", "ELECDATE", "EPIEND", "EPISTART")
integer_cols = c(
  "DIAG_COUNT", "ELECDUR", "ELECDUR_CALC", "EPIDUR", "EPIORDER",
  "EPISTAT", "EPITYPE", "FAE", "FAE_EMERGENCY", "FCE", "FDE", "NEODUR",
  "OPERTN_COUNT", "RANK_ORDER", "SEX", "SPELBGIN", "SPELDUR", "SPELDUR_CALC",
  "STARTAGE", "WAITLIST"
)


# Load dataset (combine financial years if applicable)
hes_apc_files = list.files(hes_apc_folder, pattern = "*.csv")
hes_apc = hes_apc_files %>%
  map_dfr(
    function(.hes_apc_file){
      .hes_apc = read_csv(
        here::here(hes_apc_folder, .hes_apc_file),
        col_types = cols(.default = col_character())
      )
      return(.hes_apc)
    }
  )

# Set column types
set_column_types = function(.hes_apc, .date_cols, .integer_cols, .numeric_cols){
  
  .col_names = colnames(.hes_apc)
  .date_cols = intersect(.col_names, date_cols)
  .integer_cols = intersect(.col_names, integer_cols)
  .numeric_cols = intersect(.col_names, numeric_cols)
  
  
  if(length(.date_cols) > 0){
    .hes_apc = .hes_apc %>% 
      mutate(across(all_of(.date_cols), as.Date))
  }
  
  if(length(.integer_cols) > 0){
    .hes_apc = .hes_apc %>% 
      mutate(across(all_of(.integer_cols), as.integer))
  }
  
  if(length(.numeric_cols) > 0){
    .hes_apc = .hes_apc %>% 
      mutate(across(all_of(.numeric_cols), as.numeric))
  }
  
  return(.hes_apc)
}

hes_apc = hes_apc %>%
  set_column_types(
    .date_cols = date_cols,
    .integer_cols = integer_cols,
    .numeric_cols = numeric_cols
  )

# Remove null dates
remove_null_dates = function(.date, null_dates){
  .date = if_else(.date %in% ymd(null_dates), NA_Date_, .date)
  return(.date)
}

hes_apc = hes_apc %>% 
  mutate(
    across(all_of(date_cols), function(.x) {
      remove_null_dates(.x, null_dates = c("1800-01-01", "1801-01-01"))
    }
    )
  )

# Clean column names and assign person_id column
hes_apc = hes_apc %>% 
  rename(person_id = person_id_variable) %>% 
  janitor::clean_names()

# Accept EPISTART as ADMIDATE if ADMIDATE is missing and EPIORDER is 1
hes_apc = hes_apc %>% 
  mutate(
    admidate = if_else(
      condition = is.na(admidate) & (!is.na(epistart)) & (epiorder == 1),
      true = epistart,
      false = admidate
    )
  )

# Create quality indicators
hes_apc = hes_apc %>% 
  mutate(
    known_person_id = if_else(!is.na(person_id), TRUE, FALSE),
    known_epikey = if_else(!is.na(epikey), TRUE, FALSE),
    known_procode5 = if_else(!is.na(procode5), TRUE, FALSE),
    known_epistart = if_else(!is.na(epistart), TRUE, FALSE),
    known_epiend = if_else(!is.na(epiend), TRUE, FALSE),
    known_admidate = if_else(!is.na(admidate), TRUE, FALSE),
    complete_episode = if_else(epistat == 3, TRUE, FALSE, missing = FALSE)
  )

# Create inclusion flags
hes_apc = hes_apc %>%
  mutate(
    c0 = TRUE,
    c1 = c0 & known_person_id,
    c2 = c1 & known_epikey,
    c3 = c2 & known_procode5,
    c4 = c3 & complete_episode,
    c5 = c4 & known_epistart,
    c6 = c5 & known_epiend,
    c7 = c6 & known_admidate,
    include = c7
  )

# Compute flowchart
flowchart_hes_apc = hes_apc %>% 
  select(person_id, c0, c1, c2, c3, c4, c5, c6, c7) %>% 
  mutate(row_id = row_number()) %>% 
  pivot_longer(cols = starts_with("c"), names_to = "criteria", values_to = "inclusion") %>%
  filter(inclusion) %>% 
  group_by(criteria) %>%
  summarise(
    n_episodes = n(),
    n_ids = n_distinct(person_id),
  ) %>%
  ungroup() %>% 
  mutate(
    description = case_when(
      criteria == "c0" ~ "Original HES-APC dataset",
      criteria == "c1" ~ " non-null person_id",
      criteria == "c2" ~ " non-null epikey",
      criteria == "c3" ~ " non-null procode5",
      criteria == "c4" ~ " complete episode",
      criteria == "c5" ~ " non-null epistart",
      criteria == "c6" ~ " non-null epiend",
      criteria == "c7" ~ " non-null admidate"
    ),
    episodes_removed = n_episodes - lag(n_episodes),
    ids_removed = n_ids - lag(n_ids),
    pct_episodes_removed = round(episodes_removed / lag(n_episodes) * 100, 2),
    pct_ids_removed = round(ids_removed / lag(n_ids) * 100, 2),
  ) %>% 
  select(criteria, description, n_episodes, episodes_removed, pct_episodes_removed,
         n_ids, ids_removed, pct_ids_removed)

# Save flowchart
write_csv(
  x = flowchart_hes_apc,
  file = here::here("outputs", "flowchart_hes_apc.csv")
)

# Save excluded rows for reference
hes_apc_excluded_rows = hes_apc %>% 
  filter(!include)

write_rds(
  x = hes_apc_excluded_rows,
  file = here::here("data", "hes_apc_excluded_rows.rds")
)

# Filter out bad rows and save to file
hes_apc_prepared = hes_apc %>% 
  filter(include)

write_rds(
  x = hes_apc_prepared,
  file = here::here("data", "hes_apc_prepared.rds")
)


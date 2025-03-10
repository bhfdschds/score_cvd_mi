# SCORE-CVD: Myocardial infarction alogrithm 
# Generate HES-APC Continuous Inpatient Spells (CIPS)
# 02_hes_apc_generate_cips.R
# BHF Data Science Centre, 2025
#
# Authors:
# - James Farrell, BHF Data Science Centre
# 
# Date Created: 2025-03-08
# Last updated: 2025-03-08
# Version:      v0.1
#
# This script generates continuous inpatient spell (CIPS) indicators based on 
# the combined methodologies from:
# - Health & Social Care Information Centre (2014)
#   https://webarchive.nationalarchives.gov.uk/ukgwa/20180307232845tf_/http://content.digital.nhs.uk/media/11859/Provider-Spells-Methodology/pdf/Spells_Methodology.pdf
# - Centre for Health Economics, University of York (2021)
#   https://www.york.ac.uk/media/che/documents/papers/researchpapers/CHERP182_NHS_update2018_2019_supplementary.pdf
# - Fiona Grimm, The Health Foundation Analytics Lab
#   https://github.com/HFAnalyticsLab/HES_pipeline/blob/master/src/spells.R


library(tidyverse)

# Parameters 
## Define transit codes in admisorc, admimeth and disdest
transfer_codes_admisorc = c("51", "52", "53")
transfer_codes_admimeth = c("2B", "81")
transfer_codes_disdest = c("51", "52", "53")

# Load dataset
hes_apc_prepared = read_rds(here::here("data", "hes_apc_prepared.rds"))

# Generate transfer flag
hes_apc_episodes = hes_apc_prepared %>% 
  mutate(
    transit = case_when(
      
      (!admisorc %in% transfer_codes_admisorc)
      & (!admimeth %in% transfer_codes_admimeth)
      & (disdest %in% transfer_codes_disdest) ~ 1,
      
      ((admisorc %in% transfer_codes_admisorc) | (admimeth %in% transfer_codes_admimeth))
      & (disdest %in% transfer_codes_disdest) ~ 2,
      
      ((admisorc %in% transfer_codes_admisorc) | (admimeth %in% transfer_codes_admimeth))
      & (!disdest %in% transfer_codes_disdest) ~ 3,
      
      .default = 0
    )
  )

# Select columns and arrange
hes_apc_episodes = hes_apc_episodes %>% 
  arrange(person_id, procode5, epistart, epiend, epiorder, transit, epikey) %>% 
  relocate(person_id, procode5, epistart, epiend, epiorder, transit, epikey) %>% 
  select(epikey, person_id, epistart, epiend, epiorder, epistat,
         admidate, disdate, procode5, admisorc, admimeth, disdest, dismeth, transit)


hes_apc_episodes = hes_apc_episodes %>%
  group_by(person_id, procode5) %>% 
  mutate(
    previous_admidate = lag(admidate),
    previous_epistart = lag(epistart),
    previous_epiend = lag(epiend),
    previous_dismeth = lag(dismeth)
  )

# An episode is considered to be part of the same provider spell
# as the previous episode if any one of the following is true:
# 1. `admidate` of the current episode is the same as for the
#    previous episode
# 2. `epistart` of the current episode is the same as for the
#    previous episode
# 3. The method of discharge of the previous episode is an intra-
#    provider transfer between consultants (`dismeth` is 8 or 9)
#    and the episode start date (epistart) matches the episode end
#    date of the previous episode (epiend)

hes_apc_episodes = hes_apc_episodes %>% 
  mutate(
    new_p_spell = case_when(
      admidate == previous_admidate ~ 0,
      epistart == previous_epistart ~ 0,
      (previous_dismeth %in% c(8, 9))
      & (epistart == previous_epiend) ~ 0,
      .default = 1
    ),
    p_spell_order = cumsum(new_p_spell)
  ) %>% 
  ungroup() %>% 
  mutate(
    p_spell_id = paste0(
      person_id, "-", procode5, "-", p_spell_order
    )
  )


# Calculate episode order, episode count, first and last episode flags within each provider spell
hes_apc_episodes = hes_apc_episodes %>% 
  group_by(p_spell_id) %>% 
  arrange(epistart, epiend, epiorder, transit, epikey) %>% 
  mutate(
    p_spell_epiorder = row_number(),
    p_spell_epicount = n()
  ) %>% 
  ungroup() %>% 
  mutate(
    p_spell_first_episode = if_else(p_spell_epiorder == 1, 1, 0),
    p_spell_last_episode = if_else(p_spell_epiorder == p_spell_epicount, 1, 0)
  )

p_spell_first_episodes = hes_apc_episodes %>% 
  filter(p_spell_first_episode == 1) %>% 
  select(
    person_id, procode5, p_spell_id, p_spell_order,
         p_spell_epistart = epistart,
         p_spell_admidate = admidate,
         p_spell_admisorc = admisorc,
         p_spell_admimeth = admimeth
  )

p_spell_last_episodes = hes_apc_episodes %>% 
  filter(p_spell_last_episode == 1) %>% 
  select(
    person_id, procode5, p_spell_id, p_spell_order,
    p_spell_epiend = epiend,
    p_spell_disdate = disdate,
    p_spell_disdest = disdest,
    p_spell_dismeth = dismeth
  )

hes_apc_provider_spells = p_spell_first_episodes %>% 
  full_join(
    p_spell_last_episodes,
    by = c("person_id", "procode5", "p_spell_order", "p_spell_id")
  )


hes_apc_provider_spells = hes_apc_provider_spells %>% 
  arrange(person_id, p_spell_admidate, p_spell_disdate, procode5, p_spell_order) %>% 
  group_by(person_id) %>% 
  mutate(
    prev_p_spell_epiend = lag(p_spell_epiend),
    prev_p_spell_disdest = lag(p_spell_disdest)
  ) %>% 
  ungroup()

# A provider spell is considered to be part of the same CIPS as the previous
# provider spell if `epistart` is not more than 3 days later than `epiend` of the
# previous spell and at least one of the following is true:
# 1. The discharge dstination of the previous spell is another hospital (`disdest`
# is 51, 52, or 53)
# 2. The sorce of admission of the current spell is another hospital (`admisorc`
# is 51, 52, or 53)
# 3. The method of admission of the current spell is a transfer ('admimeth' is 2B,
# or 81)

hes_apc_provider_spells = hes_apc_provider_spells %>% 
  mutate(
    spell_date_difference = as.numeric(p_spell_epistart - prev_p_spell_epiend),
    new_cips = case_when(
      (spell_date_difference <= 3)
      & (spell_date_difference >= 0)
      & (
        (prev_p_spell_disdest %in% transfer_codes_disdest)
        | (p_spell_admisorc %in% transfer_codes_admisorc)
        | (p_spell_admimeth %in% transfer_codes_admimeth)
      ) ~ 0,
      .default = 1
    )
  ) %>% 
  group_by(person_id) %>% 
  mutate(
    cips_order = cumsum(new_cips)
  ) %>% 
  ungroup() %>% 
  mutate(
    cips_id = paste0(
      person_id, "-", cips_order
    )
  )

hes_apc_provider_spells = hes_apc_provider_spells %>% 
  group_by(cips_id) %>% 
  arrange(p_spell_admidate, p_spell_disdate, procode5, p_spell_order) %>% 
  mutate(
    cips_spell_order = row_number(),
    cips_spell_count = n(),
    cips_first_spell = if_else(cips_spell_order == 1, 1, 0),
    cips_last_spell = if_else(cips_spell_order == cips_spell_count, 1, 0)
  ) %>% 
  ungroup()

cips_first_spell = hes_apc_provider_spells %>% 
  filter(cips_first_spell == 1) %>% 
  select(
    person_id, cips_id, cips_order,
    cips_epistart = p_spell_epistart,
    cips_admidate = p_spell_admidate,
    cips_admisorc = p_spell_admisorc,
    cips_admimeth = p_spell_admimeth
  )

cips_last_spell = hes_apc_provider_spells %>% 
  filter(cips_last_spell == 1) %>% 
  select(
    person_id, cips_id, cips_order,
    cips_epiend = p_spell_epiend,
    cips_disdate = p_spell_disdate,
    cips_disdest = p_spell_disdest,
    cips_dismeth = p_spell_dismeth
  )

hes_apc_cips = cips_first_spell %>% 
  full_join(
    cips_last_spell,
    by = c("person_id", "cips_order", "cips_id")
  )

# Join cips_id to prepared HES-APC dataset
hes_apc_prepared_cips = hes_apc_prepared %>%
  full_join(
    hes_apc_episodes %>% 
      select(person_id, epikey, p_spell_id),
    by = c("person_id", "epikey")
  ) %>% 
  full_join(
    hes_apc_provider_spells %>% 
      select(person_id, p_spell_id, cips_id),
    by = c("person_id", "p_spell_id")
  )

# Write to file
write_rds(
  x = hes_apc_prepared_cips,
  file = here::here("data", "hes_apc_prepared_cips.rds")
)

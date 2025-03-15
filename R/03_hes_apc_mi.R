# SCORE-CVD: Myocardial infarction alogrithm 
# Generate HES-APC Continuous Inpatient Spells (CIPS)
# 03_hes_apc_mi.R
# BHF Data Science Centre, 2025
#
# Authors:
# - James Farrell, BHF Data Science Centre
# 
# Date Created: 2025-03-08
# Last updated: 2025-03-08
# Version:      v0.1
#
# This script identifies MI events in HES-APC based on the algorithm from
# the RIPCORD-2 trial (https://www.ahajournals.org/doi/10.1161/CIRCULATIONAHA.121.057793)

library(tidyverse)
library(lubridate)

hes_apc_prepared_cips = read_rds(here::here("data", "hes_apc_prepared_cips.rds"))

# Select columns
hes_apc_selected = hes_apc_prepared_cips %>% 
  select(
    person_id, epikey, cips_id, epistart, epiend, epiorder, admidate, disdate,
    admimeth, admisorc, dismeth, disdest, diag_3_concat
  )

# Arrange rows
hes_apc_selected = hes_apc_selected %>%
  arrange(person_id, admidate, epistart, epiend, epiorder, epikey) 

# Create diagnosis flags
hes_apc_selected = hes_apc_selected %>% 
  mutate(
    diag_i21 = if_else(
      is.na(diag_3_concat),
      FALSE,
      str_detect(diag_3_concat, pattern = "I21")
    ),
    diag_i22 = if_else(
      is.na(diag_3_concat),
      FALSE,
      str_detect(diag_3_concat, pattern = "I22")
    ),
    diag_i21_or_i22 = diag_i21 | diag_i22,
    diag_i21_and_i22 = diag_i21 & diag_i22,
  ) 


# Allocate empty new columns
hes_apc_selected = hes_apc_selected %>%
  mutate(
    index_num = NA_integer_,
    first_mi_diagnosis = NA,
    qualify = NA,
    mi_date = NA_Date_,
    mi_count = NA_integer_,
    terminal_node = NA_integer_,
    terminal_node_description = NA_character_,
    last_qualifying_mi_date = NA_Date_,
    episode_lt_28d_from_mi = NA,
    prev_mi_had_i22 = NA,
    same_cips_as_last_mi = NA,
    any_gap_in_mi_diagnosis = NA
  )

# Calculate row index and whether individual has any MI codes
hes_apc_selected = hes_apc_selected %>% 
  group_by(person_id) %>% 
  mutate(
    index_num = row_number(),
    any_mi_diag = any(diag_i21_or_i22)
  ) %>% 
  ungroup()

# Dataset of individuals with no MI diagnoses - guaranteed to be assigned to terminal node 1
hes_apc_persons_no_mi = hes_apc_selected %>% 
  filter(any_mi_diag == FALSE) %>% 
  mutate(
    qualify = FALSE,
    terminal_node = 0L,
    terminal_node_description = "T0: Individual with no I.21 or I.22 diagnosis"
  )

# Dataset of individuals with at least one MI diagnosis
hes_apc_persons_with_mi = hes_apc_selected %>% 
  filter(any_mi_diag == TRUE) %>%
  group_by(person_id) %>% 
  group_modify(~ {
    
    # Compute logical flag for the first I.21 or I.22 diagnosis for this individual
    .x = .x %>%
      mutate(
        first_mi_diagnosis = if_else(
          index_num == which(diag_i21_or_i22)[1],
          true = TRUE,
          false = FALSE,
          missing = FALSE
        )
      )
    
    # Loop over rows
    for(i in 1:nrow(.x)){
      
      # Index of last valid MI event
      index_last_valid_mi <- tail(which(.x$qualify == TRUE), 1)
      if (length(index_last_valid_mi) == 0) {
        index_last_valid_mi <- NA_integer_
      }
      
      # D1: Does this episode have an I.21 or I.22 diagnosis?
      if (as.logical(.x[i, "diag_i21_or_i22"]) == FALSE){
        
        # No - this episode does not contain a I.21 or I.22 diagnosis code.
        
        # T1: Not an MI event
        .x[i, "qualify"] = FALSE
        .x[i, "terminal_node"] = 1
        .x[i, "terminal_node_description"] = "T1: Not an MI event"
        
      } else {
        
        # Yes - this episode contains and I.21 or I.22 diagnosis code.
        
        # D2: Is this the first I.21 or I.22 diagnosis?
        if(as.logical(.x[i, "first_mi_diagnosis"])){
          
          # Yes - this is the first occurance of an I.21 or I.22 code for this
          # individual
          
          # D3: Does this episode contain both I.21 and I.22 diagnosis?
          if(.x[i, "diag_i21_and_i22"] == FALSE){
            
            # No - this episode only contains one of I.21 or I.22 diagnosis
            
            # T2: Single MI event on episode start date
            .x[i, "qualify"] = TRUE
            .x[i, "mi_count"] = 1
            .x[i, "mi_date"] = .x[i, "epistart"]
            .x[i, "terminal_node"] = 2
            .x[i, "terminal_node_description"] = "T2: Single MI event on episode start date"
            
          } else {
            
            # Yes - this episode contains both I.21 and I.22 diagnoses
            
            # T3: Two separate MI events on episode start date
            .x[i, "qualify"] = TRUE
            .x[i, "mi_count"] = 2
            .x[i, "mi_date"] = .x[i, "epistart"]
            .x[i, "terminal_node"] = 3
            .x[i, "terminal_node_description"] = "T3: Two seperate MI events on episode start date"
            
          }
          
        } else {
          
          # No - this is not the first occurance of an I.21 or I.22 code for this
          # individual
          
          # Compute whether the episode start occured less than 28 days from a
          # recorded MI event
          .x[i, "last_qualifying_mi_date"] = .x[index_last_valid_mi, "mi_date"]
          .x[i, "episode_lt_28d_from_mi"] = as.numeric(
            .x[i, "epistart"] - .x[i, "last_qualifying_mi_date"]
          ) < 28
          
          
          # D4: Is this episode less than 28 days from an already recorded MI event?
          if (as.logical(.x[i, "episode_lt_28d_from_mi"])){
            
            # Yes - episode started less than 28 days from an already recorded
            # MI event
            
            # D5: Does this episode contain I.22?
            if(as.logical(.x[i, "diag_i22"]) == FALSE){
              
              # No - episode diagnosis does not contain I.22
              
              # T4: Not an MI event
              .x[i, "qualify"] = FALSE
              .x[i, "terminal_node"] = 4
              .x[i, "terminal_node_description"] = "T4: Not an MI event"
              
            } else {
              
              # Yes - episode diagnosis contains I.22
              
              # Compute whether the last recorded MI event contained an I.22 code
              .x[i, "prev_mi_had_i22"] = .x[index_last_valid_mi, "diag_i22"]
              
              # D6: Did the last recorded MI event contain an I.22 code?
              if(as.logical(.x[i, "prev_mi_had_i22"]) == FALSE){
                
                # No - the previous MI event did not contain an I.22 code
                
                # T5: Single MI event on episode start date
                .x[i, "qualify"] = TRUE
                .x[i, "mi_count"] = 1
                .x[i, "mi_date"] = .x[i, "epistart"]
                .x[i, "terminal_node"] = 5
                .x[i, "terminal_node_description"] = "T5: Single MI event on episode start date"
                
              } else {
                
                # Yes - the previous MI evnet did contain an I.22 code
                
                # T6: Not an MI event
                .x[i, "qualify"] = FALSE
                .x[i, "terminal_node"] = 6
                .x[i, "terminal_node_description"] = "T6: Not an MI event"
                
              }
              
            }
            
          } else {
            
            # No - episode started 28 days or more from an already recorded MI
            # event
            
            # Compute whether this episode is part of the same CIPS as the last
            # recorded MI event
            
            .x[i, "same_cips_as_last_mi"] = .x[i, "cips_id"] == .x[index_last_valid_mi, "cips_id"]
            
            # D7: Is this row part of the same continuous inpatient spell (CIPS)
            # as the previous recorded MI event?
            if(as.logical(.x[i, "same_cips_as_last_mi"]) == FALSE){
              
              # No - This episode is not part of the same CIPS as the previous
              # recorded MI event
              
              # D8: Does this episode contain both I.21 and I.22 diagnoses?
              if(as.logical(.x[i, "diag_i21_and_i22"]) == FALSE){
                
                # No - this episode only contains one of I.21 or I.22 diagnoses
                
                # T7: Single MI event on episode start date
                .x[i, "qualify"] = TRUE
                .x[i, "mi_count"] = 1
                .x[i, "mi_date"] = .x[i, "epistart"]
                .x[i, "terminal_node"] = 7
                .x[i, "terminal_node_description"] = "T7: Single MI event on episode start date"
                
              } else {
                
                # Yes - this episode contains both I.21 and I.22 diagnoses
                
                # T8: Two seperate MI events on episode start date
                .x[i, "qualify"] = TRUE
                .x[i, "mi_count"] = 2
                .x[i, "mi_date"] = .x[i, "epistart"]
                .x[i, "terminal_node"] = 8
                .x[i, "terminal_node_description"] = "T8: Two seperate MI events on episode start date"
                
              }
              
            } else {
              
              # Yes - This episode is part of the same CIPS as the previous recorded
              # MI event
              
              # Compute whether there are any episodes without I.21 or I.22 between
              # this episode and the last recorded MI event
              .x[i, "any_gap_in_mi_diagnosis"] = (
                .x %>%
                  filter(
                    index_num > index_last_valid_mi,
                    index_num < i,
                    diag_i21_or_i22 == FALSE
                  ) %>%
                  nrow() > 0
              )
              
              # D9: Is there at least one episode between the previous MI event
              # and this episode which does not contain I.21 or I.22?
              
              if(as.logical(.x[i, "any_gap_in_mi_diagnosis"]) == FALSE){
                
                # No - There are no episodes between the previous recorded MI event
                # and this episode that do not contain an I.21 or I.22 diagnosis
                
                # T9: Not an MI event
                .x[i, "qualify"] = FALSE
                .x[i, "terminal_node"] = 9
                .x[i, "terminal_node_description"] = "T9: Not an MI event"
                
              } else {
                
                # Yes - Ther is at least one episode between the previous MI
                # event and this episode which does not contain I.21 or I.22
                
                # D10: Does this episode contain both I.21 and I.22 diagnoses
                if(as.logical(.x[i, "diag_i21_and_i22"]) == FALSE){
                  
                  # No - this episode only contains one of I.21 or I.22 diagnoses
                  
                  # T10: Single MI event on episode start date
                  .x[i, "qualify"] = TRUE
                  .x[i, "mi_count"] = 1
                  .x[i, "mi_date"] = .x[i, "epistart"]
                  .x[i, "terminal_node"] = 10
                  .x[i, "terminal_node_description"] = "T10: Single MI event on episode start date"
                  
                } else {
                  
                  # Yes - this episode contains both I.21 and I.22 diagnoses
                  
                  # T11: Two seperate MI events on episode start date
                  .x[i, "qualify"] = TRUE
                  .x[i, "mi_count"] = 2
                  .x[i, "mi_date"] = .x[i, "epistart"]
                  .x[i, "terminal_node"] = 11
                  .x[i, "terminal_node_description"] = "T11: Two seperate MI events on episode start date"
                  
                }
              }
            }
          }
        }
      }
    }
    
    return(.x)
    
  }) %>% 
  ungroup()

# Combine datasets and save to file
hes_apc_processed = hes_apc_persons_no_mi %>% 
  bind_rows(hes_apc_persons_with_mi)

write_rds(
  x = hes_apc_processed,
  file = here::here("data", "hes_apc_processed.rds")
)


# Save MI events to file
hes_apc_mi_events = hes_apc_processed %>% 
  filter(qualify) %>% 
  select(person_id, mi_date, mi_count) %>% 
  uncount(mi_count) %>% 
  group_by(person_id) %>% 
  mutate(
    mi_event_index = row_number()
  ) %>% 
  ungroup()

write_rds(
  x = hes_apc_mi_events,
  file = here::here("data", "hes_apc_mi_events.rds")
)

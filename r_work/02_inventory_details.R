## NPRB Legacy Data Inventory

## Chris Turner - 2025-04-23
## GH: iamchrisser

## Script narrative
# read in 01_full_inventory.csv from script 01_inventory.R and filter down to
# look for projects that appear to be ready to be archived.
# Find all projects that have zip and xml files and look for those that with
# file_name values matching the regex:"NPRB\\.2[0-9]{3}\\.[0-9]{2}\\.[zip|xml]"

###############################################################################
############################### 02_inventory_details.R ################################
###############################################################################

## Load what's needed
if (!("pacman" %in% rownames(installed.packages()))){
  install.packages("pacman")
}
pacman::p_load(tidyverse)
source("00_inventory_functions.R")
data_dir <- "data_out/"

big_inv <- read_csv(paste0(data_dir, "01_full_inventory.csv"))

potential_projects <- big_inv %>%
  group_by(project_name) %>%
#  filter(file_name %>%
#           str_detect("NPRB\\.2[0-9]{3}\\.[0-9]{2}\\.[zip|xml]")) %>%
  summarize(n_zip = sum(mimetype == "application/zip"),
            n_xml = sum(mimetype == "application/xml")) %>%
  filter(n_zip > 1,  n_xml > 1)

# find all folders at least one zip and xml file that match the regex
potential_archives <- big_inv %>%
  group_by(project_name, project_id, path) %>%
  summarize(n_zip = sum(file_name %>%
                          str_detect("NPRB\\.2[0-9]{3}\\.[0-9]{2}\\.zip")),
            n_xml = sum(file_name %>%
                          str_detect("NPRB\\.2[0-9]{3}\\.[0-9]{2}\\.xml"))) %>%
  filter(n_zip > 0,  n_xml > 0)
  
write_csv(potential_archives, 
          timestamp_filename(data_dir, "02_potential_folders"))  

# how many projects are there with zip and xml files?
projects_summary <- data.frame(
  all_projects = length(unique(big_inv$project_name)),
  potential_archives = length(unique(potential_archives$project_name))
)

write_csv(projects_summary, timestamp_filename(data_dir, "02_projects_summary"))

# get total volume for: all projects, projects with zip and xml files
# get volume of just zip and xml for potential archives

volume_all <- get_volumes(big_inv)

pa <- potential_archives %>%
  left_join(volume_all, by = c("project_id" = "project_id")) %>% 
  select(project_name.x, project_id, path, n_zip, n_xml, n_files, total_MB)
  
file_deets <- big_inv %>%
  filter(file_name %>% str_detect("NPRB\\.2[0-9]{3}\\.[0-9]{2}\\.[zip|xml]")) %>%
  select(project_name, project_id, path, file_name, bytes)

potential_archvies <- pa %>%
  left_join(file_deets, by = c("project_id" = "project_id", "path" = "path")) %>%
  select(project_name.x, project_id, path, n_zip, n_xml, n_files, total_MB,
         file_name, bytes) %>% 
  mutate(bytes = bytes / 1000000) %>%
  rename(file_MB = bytes, project_name = project_name.x)

write_csv(potential_archvies, 
          timestamp_filename(data_dir, "02_potential_archives"))  

#volume_potential_archives <- get_volumes(potential_archives)
write_csv(volume_all, timestamp_filename(data_dir, "02_volume_all"))
#write_csv(volume_potential_archives, 
#          timestamp_filename(data_dir, "02_volume_potential_archives"))

remove(list=ls())

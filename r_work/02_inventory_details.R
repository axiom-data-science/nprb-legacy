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

big_inv <- read_csv("data_out/01_full_inventory.csv")

# get list of all projects with a zip file or an xml file
zips_xml <- big_inv %>%
  filter(mimetype == "application/xml") %>% 
  rbind(big_inv %>% filter(str_ends(file_name, ".zip"))) %>%
  arrange(project_name) %>%
  select(names(big_inv)[c(1,5,3,9,7,8,6,4,2)])

potential_projects <- big_inv %>%
  group_by(project_name) %>%
  #filter(file_name %>%
  #         str_detect("NPRB\\.2[0-9]{3}\\.[0-9]{2}\\.[zip|xml]")) %>%
  summarize(n_zip = sum(mimetype == "application/zip"),
            n_xml = sum(mimetype == "application/xml")) %>%
  filter(n_zip > 1,  n_xml > 1)

# filter down to projects with files with appropriately formatted names
potential_archives <- big_inv %>%
  filter(project_name %in% potential_projects$project_name)

# filter further to just those projects with more than 1 file
x <- count(potential_archives, project_name) |>
  filter(n > 1)

potential_archives <- potential_archives %>%
  filter(project_name %in% x$project_name) %>%
  arrange(project_name)
remove(x)

write_csv(potential_archives, "data_out/02_potential_archives.csv")


# how many projects are there with zip and xml files?
projects_summary <- data.frame(
  all_projects = length(unique(big_inv$project_name)),
  project_zip_xml = length(unique(zips_xml$project_name)),
  potential_archives = length(unique(potential_archives$project_name))
)

write_csv(projects_summary, "data_out/02_projects_summary.csv")
# get total volume for: all projects, projects with zip and xml files
# get volume of just zip and xml for potential archives


volume_all <- get_volumes(big_inv)
volume_zip_xml <- get_volumes(zips_xml)
volume_potential_archives <- get_volumes(potential_archives)
write_csv(volume_all, "data_out/02_volume_all.csv")
write_csv(volume_zip_xml, "data_out/02_volume_zip_xml.csv")
write_csv(volume_potential_archives, "data_out/02_volume_potential_archives.csv")

remove(list=ls())

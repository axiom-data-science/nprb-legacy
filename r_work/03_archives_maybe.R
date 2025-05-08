## NPRB Legacy Data Inventory

## Chris Turner - 2025-04-29
## GH: iamchrisser

## Script narrative
# 1. pull zip and xml files from the RW projects identified as having datasets 
#    likely to be ready to archive
# 2. check zip file contents and metadata record as defined in the scoping doc:
#    https://axds.atlassian.net/wiki/external/YmQzNTZjNDliZjkzNDM4NGE5ZTJiNmQ4MmJmNGQzYTI

###############################################################################
########################### 03_archives_maybe.R ###############################
###############################################################################
library(tidyverse)
source("r_work/00_inventory_functions.R")

# get the most recent version of '02_potential_archives'

newest_file <- sort(list.files("r_work/data_out", 
                               full.names = TRUE)[str_detect(list.files("r_work/data_out"),
                                "02_potential_archives")],
                    decreasing = TRUE)[1]

potential_archives <- read_csv(newest_file)

# testset <- archives |> 
#   filter(project_id %in% sample(archives$project_id, 5))

#write_csv(testset, "data_out/testset.csv")

proj_sort <- count(potential_archives, project_id, path, name="n_path") %>% 
  left_join(count(potential_archives, project_id, name="n_proj"), by=join_by(project_id))
base_path <- "./"


### rewrite this. I'm not sure that it's does what it needs to do
### that is, it finds the projects that have exactly one zip and one xml file
### and downloads those files. I also need it to get the zip and xml files from
### the projects that have more than one zip and xml file
for (i in 1:nrow(proj_sort)){
  if (proj_sort$n_proj[i] == 2 && proj_sort$n_proj[i] == 2){
    this_one <- archives |>
      filter(project_id == proj_sort$project_id[i],
             path == proj_sort$path[i])
    dir_out <- str_split(this_one$project_name[1], " ")[[1]][1]
    full_dir <- paste0(base_path, dir_out)
    if(!dir.exists(full_dir)){
      dir.create(full_dir)
    }
    #print(dir_out)
    wget_file_from_rw(full_dir, this_one$file_id[1], this_one$file_name[1])
    wget_file_from_rw(full_dir,  this_one$file_id[2], this_one$file_name[2])
  } else {
    ifelse(exists("tbd"), tbd <- rbind(tbd, this_one), tbd <- this_one)
  }
}


## Query RW DB to for projects that already have archives associated with them
pacman::p_load(DBI, RPostgreSQL)

inv <- read_csv("r_work/data_out/01_full_inventory.csv")

projects <- inv |>
  select(project_name, project_id) |>
  unique()

proj_ids <- projects %>% 
  pull(project_id) %>%
  paste(collapse = ",")

psql <- dbDriver("PostgreSQL")

con <- dbConnect(
  drv = psql,
  dbname = "research_workspace",
  host = "oltp.db.axiomptk",
  port = 5432,
  password = Sys.getenv("WORKSPACE_READ"),
  user = "workspace_read"
)

q <- paste0(
  "SELECT projectid as project_id, citation, doi, metadatapid
  FROM archivepackage
  WHERE projectid IN (", proj_ids ,")"
)

archives_in_db <- dbGetQuery(con, q)
write_csv(archives_in_db, "r_work/data_out/03_archives_in_db.csv")

archives <- left_join(projects, archives_in_db, by = "project_id") |> 
  arrange(project_name)

proj_vols <- read_csv("r_work/data_out/02_all_volumes_2025-04-29_21-54-12.csv")
p <- proj_vols |>
  left_join(archives, by = "project_id", multiple = "all") |> 
  select(project_name.x, project_id, n_files, total_MB,
         new_archive, doi) |>
  rename(project_name = project_name.x, rw_archive = doi)

multi_archives <- p |>
  filter(project_name %in% (p |> count(project_name) |>
                              filter(n > 1))["project_name"]$project_name) |> 
  drop_na(rw_archive)


archive_inventory <- p |>
  filter(!project_name %in% multi_archives$project_name) |> 
  bind_rows(multi_archives)
write_csv(archive_inventory, "r_work/data_out/03_archive_inventory.csv")

remove(list=ls())

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

archives <- read_csv(newest_file)

# testset <- archives |> 
#   filter(project_id %in% sample(archives$project_id, 5))

#write_csv(testset, "data_out/testset.csv")

proj_sort <- count(archives, project_id, path, name="n_path") %>% 
  left_join(count(archives, project_id, name="n_proj"), by=join_by(project_id))


for (i in 1:10){
  if (proj_sort$n_proj[i] == 2 && proj_sort$n_proj[i] == 2){
    this_one <- archives |>
      filter(project_id == proj_sort$project_id[i],
             path == proj_sort$path[i])
    dir_out <- str_split(this_one$project_name[1], " ")[[1]][1]
    full_dir <- paste0("/home/rstudio/data/", dir_out)
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



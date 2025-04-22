## NPRB Legacy Data Inventory

## Chris Turner - 2025-04-22
## GH: iamchrisser

## {This is a description of the project and this code script.}
## {This is a description of the project and this code script.}
## {This is a description of the project and this code script.}

###############################################################################
############################### 01-inventory.R ################################
###############################################################################

## Load what's needed
if (!("pacman" %in% rownames(installed.packages()))) {
  install.packages("pacman")
}
pacman::p_load(tidyverse, DBI, RPostgreSQL, keyring)

source("01_nprb-inventory_functions.R")

## Narrative summary of the code
# search RW DB for projects in the NPRB Org that begin with '0[0-9]{3}' or with 
# '1[1-4][0-9]{2}' to find all projects funded under NPRB Core program from 
# years before Axiom took over as the data management contractor for NPRB.
#
# For each project identified, look for: 
# 1. two documents in the files folder named like:
#      'NPRB\.2[0-9]{3}\.[0-9]{2}\.[xml|.zip]'
#   a. check for the existence of a subfolder called 'data not described'
#   b. check for more than one zip or xml file in that folder.
#
# So to start:
# 1. get all matching projects. 
# 2. get all files
# 3. get folder path for all files

## use RW db to get all projects in NPRB organization that match the regex
# 1. specify the db drive
# 2. define the connection
#    Using keyring to provide credentials
#    https://solutions.posit.co/connections/db/best-practices/managing-credentials/
# 3. write a query to get the files in each folder associated with a submitted package

psql <- dbDriver("PostgreSQL")

con <- dbConnect(
  drv = psql,
  dbname = "research_workspace",
  host = "oltp.db.axiomptk",
  port = 5432,
  password = key_get("research_workspace", keyring = "dbs"),
  user = key_list("research_workspace", keyring = "dbs")[1,2]
)

# Query RW db to get id and names for all projects in NPRB 
# organization that match the regex
q <- paste0(
  "SELECT id as proj_id, name as proj_name 
  FROM project
  WHERE project.id IN 
    (SELECT project_id
    FROM projectorgrole
    WHERE projectorgrole.org_id IN
      (SELECT id
      FROM organization 
      WHERE organization.name='North Pacific Research Board'))
  AND project.name ~ '^0[0-9]{3}[A-Za-z]* '
  OR project.name ~ '^1[1-4]{1}[0-9]{2}[A-Za-z]* '"
)

project_names <- dbGetQuery(con, q)

## get all files in each project
proj_ids <- project_names %>% 
  pull(proj_id) %>%
  paste(collapse = ",")

q <- paste0(
  "SELECT id, bytes, filename, mimetype, folder_id, project_id 
  FROM document
  WHERE project_id IN (", proj_ids ,") 
    AND deleted IS NULL
    AND folder_id IS NOT NULL"
)
remove(proj_ids)
all_docs <- dbGetQuery(con, q)

names(all_docs)[1] <- "file_id"
names(all_docs)[3] <- "file_name"

## get all folders for all files
folder_ids <- all_docs %>% 
  pull(folder_id) %>%
  paste(collapse = ",") %>% 
  unique()

q <- paste0(
  "SELECT id, name, parent, project_id
  FROM folder
  WHERE id in (", folder_ids, ")"
)
remove=(folder_ids)
#file_folders <- dbGetQuery(con, q)

all_folders <- get_parents(dbGetQuery(con, q))
names(all_folders)[1:3] <- c("folder_id", "folder_name", "parent_id")


docs_w_folders <- all_docs %>% 
  left_join(all_folders, by = c("folder_id" = "folder_id")) %>% 
  select(project_id.x, folder_id, folder_name, file_name, bytes, mimetype, parent_id)

remove(all_docs)

## create paths in all_folders table

af_copy <- all_folders %>% 
  select(folder_id, parent_id)

af_copy <- get_next_gen(af_copy)

names(af_copy)

## get folder names from ids

# can I get rid of copy_df?
copy_df <- get_folder_names(af_copy, all_folders)
remove(af_copy)
names(copy_df) <- make.names(names(copy_df), unique = TRUE)
#names(copy_df)

all_paths <- make_path(copy_df) %>% 
  select("folder_depth_1", "path")

# can I get rid of the all_folders2?
all_folders <- all_folders %>% 
  left_join(all_paths, by = c("folder_id" = "folder_depth_1"))
#all_folders2 <- all_folders %>% 
  left_join(all_paths, by = c("folder_id" = "folder_depth_1"))

## get project names
# I didn't save these earlier, so I'd need to go back to the DB 
# or create another table earlier in the script

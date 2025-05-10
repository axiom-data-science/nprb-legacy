## NPRB Legacy Data Inventory

## Chris Turner - 2025-04-22
## GH: iamchrisser

## Script narrative
# Search RW DB for projects in the NPRB Org that begin with '0[0-9]{3}' 
# or with '1[1-4][0-9]{2}' to find all projects funded under NPRB Core 
# program from years before Axiom took over as the data management contractor 
# for NPRB. For each project, get information on all files in the project and 
# create `01_full_inventory.csv` with the following columns:
# `project_name`, `project_id`, `folder_name`, `folder_id`, `file_name`,
# `file_id`, `bytes`, `mimetype`, `path`.

###############################################################################
############################### 01-inventory.R ################################
###############################################################################

## Load what's needed
if (!("pacman" %in% rownames(installed.packages()))){
  install.packages("pacman")
}
pacman::p_load(tidyverse, DBI, RPostgreSQL)

source("00_inventory_functions.R")

# connect to the RW db
psql <- dbDriver("PostgreSQL")

con <- dbConnect(
  drv = psql,
  dbname = "research_workspace",
  host = "oltp.db.axiomptk",
  port = 5432,
  password = Sys.getenv("WORKSPACE_READ"),
  user = "workspace_read"
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
  OR project.name ~ '^1[1-4]{1}[0-9]{2}[A-Za-z]* '
  AND project.deleted IS NULL"
)

project_names <- arrange(dbGetQuery(con, q), proj_name)


## get all files in each project
proj_ids <- project_names %>% 
  pull(proj_id) %>%
  paste(collapse = ",")

q <- paste0(
  "SELECT id as file_id, bytes, filename as file_name, mimetype, folder_id, project_id 
  FROM document
  WHERE project_id IN (", proj_ids ,") 
    AND deleted IS NULL
    AND folder_id IS NOT NULL"
)

all_docs <- dbGetQuery(con, q)
remove(proj_ids)

#names(all_docs)[1] <- "file_id"
#names(all_docs)[3] <- "file_name"

## get all folders for all files
folder_ids <- all_docs %>% 
  pull(folder_id) %>%
  paste(collapse = ",") %>% 
  unique()

q <- paste0(
  "SELECT id as folder_id, name as folder_name, parent as parent_id, project_id
  FROM folder
  WHERE id in (", folder_ids, ")"
)
all_folders <- get_parents(dbGetQuery(con, q))
remove(folder_ids)
dbDisconnect(con)

#names(all_folders)[1:3] <- c("folder_id", "folder_name", "parent_id")

docs_w_folders <- all_docs %>% 
  left_join(all_folders, by = c("folder_id" = "folder_id")) %>% 
  select(project_id.x, folder_id, folder_name, file_id, 
         file_name, bytes, mimetype, parent_id)
remove(all_docs)

## create paths in all_folders table
af_copy <- all_folders %>% 
  select(folder_id, parent_id)
another_copy <- af_copy
af_copy <- get_next_gen(af_copy, another_copy)

## get folder names from ids
another_copy <- get_folder_names(af_copy, all_folders)
names(another_copy) <- make.names(names(another_copy), unique = TRUE)

all_paths <- make_path(another_copy) %>% 
  select("folder_depth_1", "path")
remove(another_copy)

all_folders <- all_folders %>% 
  left_join(all_paths, by = c("folder_id" = "folder_depth_1"))

## get project names
docs_w_folders <- docs_w_folders %>% 
  left_join(project_names, by = c("project_id.x" = "proj_id")) %>% 
  rename(project_name = proj_name, project_id = project_id.x) %>%
  select(project_name, project_id, folder_name, folder_id, file_name, 
         file_id, bytes, mimetype)

full_inventory <- docs_w_folders %>% 
  left_join(all_folders[c("folder_id", "path")], by = c("folder_id" = "folder_id")) %>%
  select(project_name, project_id, folder_name, folder_id, file_name, 
         file_id, bytes, mimetype, path)
remove(docs_w_folders)
summary(full_inventory)

write_csv(full_inventory, "01_full_inventory.csv")
remove(list=ls())

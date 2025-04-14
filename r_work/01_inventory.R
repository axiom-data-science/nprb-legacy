if (!("pacman" %in% rownames(installed.packages()))) {
  install.packages("pacman")
}
pacman::p_load(tidyverse, DBI, RPostgreSQL, keyring)

## narrative summary of the code
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
# So I guess:
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

q <- paste0(
  "SELECT id as org_id FROM organization o WHERE o.name='North Pacific Research Board'"
)
org_id <- dbGetQuery(con, q)[[1]]
# org_id

q <- paste0(
  "SELECT id as project_id, name as project_name 
  FROM project
  WHERE project.id IN 
    (SELECT project_id 
    FROM projectorgrole 
    WHERE projectorgrole.org_id=", org_id,")
    AND project.name ~ '0[0-9]{3}' 
    OR project.name ~ '1[1-4][0-9]{2}'"
)
j <- dbGetQuery(con, q)
# j[1,2] 

k <- j %>% filter(str_starts(project_name, '0[0-9]{3} '))
j <- j %>% filter(str_starts(project_name, '1[1-4][0-9]{2} '))
j <- bind_rows(j, k)
remove(k)
j[2]

## get all files in each project, try this:
# SELECT id, bytes, filename, mimetype, folder_id, project_id 
# FROM document
# WHERE project_id IN j[1]
proj_ids <- j %>% 
  pull(project_id) %>%
  paste(collapse = ",")

q <- paste0(
  "SELECT id, bytes, filename, mimetype, folder_id, project_id 
  FROM document
  WHERE project_id IN (", proj_ids ,") 
    AND deleted IS NULL
    AND folder_id IS NOT NULL"
)
all_docs <- dbGetQuery(con, q)

## get all folders for all files, try something like:
# SELECT id, name, parent, project_id
# FROM folder
# WHERE id in (", all_folders, ")"

folder_ids <- all_docs %>% 
  pull(folder_id) %>%
  paste(collapse = ",") %>% 
  unique()

q <- paste0(
  "SELECT id, name, parent, project_id
  FROM folder
  WHERE id in (", folder_ids, ")"
)

all_folders <- dbGetQuery(con, q)

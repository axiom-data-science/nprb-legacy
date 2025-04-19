if (!("pacman" %in% rownames(installed.packages()))) {
  install.packages("pacman")
}
remove(list=ls())
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

j <- dbGetQuery(con, q)

## get all files in each project, try this:
# SELECT id, bytes, filename, mimetype, folder_id, project_id 
# FROM document
# WHERE project_id IN j[1]
proj_ids <- j %>% 
  pull(proj_id) %>%
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

file_folders <- dbGetQuery(con, q)

get_parents <- function(folders_df) {
  orphans <- folders_df %>% 
    filter(!parent %in% folders_df$id) %>% 
    filter(!is.na(parent))  
  
  if (nrow(orphans) == 0) {
    return(folders_df)
  } else {
    parent_ids <- orphans$parent %>% 
    paste(collapse = ",") %>% 
    unique()  
  
    q <- paste0(
    "SELECT id, name, parent, project_id
    FROM folder
    WHERE id in (", parent_ids, ")"
    )
    parents <- dbGetQuery(con, q)
    temp_folders <- rbind(folders_df, parents)
    
    return(get_parents(temp_folders))
  }
}

all_folders <- get_parents(file_folders)

names(all_docs)[1] <- "file_id"
names(all_docs)[3] <- "file_name"
names(all_docs)
names(all_folders)[1:3] <- c("folder_id", "folder_name", "parent_id")
names(all_folders)

docs_w_folders <- all_docs %>% 
  left_join(all_folders, by = c("folder_id" = "folder_id")) %>% 
  select(project_id.x, folder_id, folder_name, file_name, bytes, mimetype, parent_id)

# create paths in all_folders table
names(all_folders)


folder_goofin <- all_folders %>% 
  select(folder_id, parent_id)
copy_df <- folder_goofin

get_next_gen <- function(folders_df, cloned_df){
  folders_df <- left_join(folders_df, cloned_df, by = c("parent_id" = "folder_id"),
                          keep = FALSE, na_matches = "never", multiple = "any")
  print(names(folders_df))
  new_names <- c()
  for (i in 1:ncol(folders_df)){
    new_names[i] <- paste0("folder_depth_", i)
  }
  names(folders_df) <- new_names
  names(folders_df)[ncol(folders_df)] <- "parent_id"
  if (all(is.na(folders_df$parent_id))){
    return(folders_df)
  }
  else {
    folders_df <- get_next_gen(folders_df, cloned_df)
  }
}

folder_goofin <- get_next_gen(folder_goofin, copy_df)





# get project names
!is.na(all_folders$parent_id)[1:25]

all_folders <- all_folders %>% 
  left_join(j, by = c("project_id" = "proj_id")) %>% 
  select(folder_id, folder_name, parent_id, project_id, project_name)

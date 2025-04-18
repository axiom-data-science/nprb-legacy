if (!("pacman" %in% rownames(installed.packages()))) {
  install.packages("pacman")
}
pacman::p_load(tidyverse, DBI, RPostgreSQL, keyring)
remove(list=ls())
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
names(all_docs)
names(all_folders)[3] <- "parent_id"
names(all_folders)

docs_w_folders <- all_docs %>% 
  left_join(all_folders, by = c("folder_id" = "folder_id")) %>% 
  select(project_id.x, folder_id, folder_name, filename, bytes, mimetype, parent_id)

# create paths in all_folders table
names(all_folders)

folder_goofin <- all_folders %>% 
  select(folder_id, folder_name, project_id, parent_id, )

num_cols <- ncol(folder_goofin)
folder_goofin[paste0("p",num_cols+1)] <- NA

get_parent_info <- function(folders_df){
  num_cols <- ncol(folders_df)
  folders_df[paste0("p",num_cols+1)] <- NA
  for (i in 1:nrow(folders_df)){
    if (is.na(folders_df$parent_id[i])){
      next
    }
    else {
      folders_df$p2[i] <- 
        folders_df$parent_id[folders_df$folder_id == folders_df$parent_id[i]]
    }
  }
  ncol(folders_df)
  if (!is.na(all(folders_df[,ncol(folders_df)]))){
    get_parent_info(folders_df)
  } else {
    return(folders_df)
  }
  
}




all_folders$folder_id[all_folders$folder_id == 371709]
names(folder_goofin)
folder_goofin$parent2 <- folder_goofin$folder_id[]

sample_n(all_folders, 10)

all_folders$path <- NA

create_paths <- function(folders_df){
  for (i in 1:nrow(folders_df)){
    if (is.na(folders_df$parent_id[i])){
      next
    }
    parent_info <- get_parent_info(folders_df, folders_df$parent_id[i])
    while(!is.na(parent_info$parent_id)){
      folders_df$path[i] <- paste0(parent_info$folder_name, "/", folders_df$path[i])
      parent_info <- get_parent_info(folders_df, parent_info$parent_id)
    }
  }
}

get_parent_info <- function(folders_df, parents_id){
  parent_deets <- folders_df[folders_df$folder_id == parents_id,]
  return(parent_deets)
}

create_paths <- function(folders_df){
  for (i in 1:nrow(folders_df)){
    if (is.na(folders_df$parent_id[i])){
      next
    }
    else {
      parent_info <- get_parent_info(folders_df, folders_df$parent_id[i])
      while(!is.na(parent_info$parent_id)){
        folders_df$path[i] <- paste0(parent_info$folder_name,"/",folders_df$path[i])
        parent_info <- get_parent_info(folders_df, parent_info)
      }
    }
  }
}

names(all_folders)
all_f2 <- create_paths(all_folders)
  

# get project names
!is.na(all_folders$parent_id)[1:25]

all_folders <- all_folders %>% 
  left_join(j, by = c("project_id" = "proj_id")) %>% 
  select(folder_id, folder_name, parent_id, project_id, project_name)

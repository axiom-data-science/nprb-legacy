## NPRB Legacy Data Inventory

## Chris Turner - 2025-04-22
## GH: iamchrisser

## Script narrative
# A collection of helper functions used in `01_inventory.R`

###############################################################################
########################### 01_inventory_functions.R ##########################
###############################################################################

#' Get the parent folder ids
#' 
#' This function retrieves the parent folders of a given folder in a database.
#' It uses a recursive approach to find all parent folders until no more parents
#' are found, with a separate DB query for each recursive function call.
#' 
#' @param folders_df a data frame with folder information,
#' including id, name, parent, and project_id.
#' 
#' @noRd
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

#' Get the all folders in the file path
#' 
#' This function retrieves all of the folders in the file path of a given folder.
#' It uses a recursive approach to find all folders until no more folders, and 
#' returns a data frame with the folder ids and names. There's probably a more
#' efficient way to do this, but this works.
#' 
#' @param folders_df a data frame with folder_id and parent_id
#' 
#' @noRd
get_next_gen <- function(folders_df, cloned_df){
  folders_df <- left_join(folders_df, cloned_df, by = c("parent_id" = "folder_id"),
                          keep = FALSE, na_matches = "never", multiple = "any")
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

#' Get the folder names for df of folder ids
#' 
#' This function retrieves the names of all folders for which there are ids in
#' the `dir_tree_ids` data frame. Again, there's probably a more
#' efficient way to do this, but this works.
#' 
#' @param dir_tree_ids a data frame of folder_ids for each folder in the path
#' @param folders_df a data frame with folder_id and folder_name
#' 
#' @noRd
get_folder_names <- function(dir_tree_ids, folders_df){
  cloned_df <- dir_tree_ids
  n <- ncol(dir_tree_ids)
  for (i in 1:n){
    w <- dir_tree_ids[,i]
    
    folder_names <- replicate(nrow(dir_tree_ids), NA)
    names(folder_names) <- paste0("folder_names_", i)
    #print(names(folder_names))
    for (j in 1:length(w)){
      if (!is.na(w[[j]])){
        folder_names[j] <- folders_df$folder_name[folders_df$folder_id == w[[j]]]
      }
    }
    
    this <- which(colnames(cloned_df) == names(dir_tree_ids)[i])
    the_next <- this + 1
    
    ifelse(i < n, 
           cloned_df <- cbind(cloned_df[1:this], folder_names, 
                            cloned_df[the_next:ncol(cloned_df)]), 
           cloned_df <- cbind(cloned_df, folder_names))
  }
  return(cloned_df)
}

#' Combine folder names into a path
#' 
#' This function combines the folder names into a path for each folder in the 
#' `folder_df`` data frame. It proceeds through each row and column of the data
#' frame and concatenates the folder names into a single string, separated by
#' "/". The function skips `NA` values (no folder) and numeric values (these are
#' folder ids. Again, there's probably a more efficient way to do this.
#' 
#' @param folders_df a wide data frame with folder_ids and folder_names for 
#' each folder in a path. 
#' 
#' @noRd
make_path <- function(folder_df){
  for (i in 1:nrow(folder_df)){
    path <- ""
    for (j in 1:ncol(folder_df)){
      if (!is.na(folder_df[i,j]) && !is.numeric(folder_df[i,j])){
        path <- paste0(folder_df[i,j], "/", path)
      }
    }
    folder_df$path[i] <- path
  }
  return(folder_df)
}

#' Get volume for all files in each project
#' 
#' This function uses group_by to summarise the total volume and number of files
#' in each project in the `some_df` data frame. It returns a data frame with
#' the project name, number of files, and total volume in bytes.
#' 
#' @param some_df a data frame with columns project_name, bytes, and file_name
#' 
#' @noRd
get_volumes <- function(some_df){
  some_df %>%
    group_by(project_name) %>%
    summarise(total_volume = sum(bytes),
              n_files = n()) %>%
    select(project_name, n_files, total_volume)
}

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
    group_by(project_name, project_id) %>%
    summarise(total_MB = round(sum(bytes) / 100000, 3),
              n_files = n()) %>%
    select(project_name, project_id, n_files, total_MB)
}


#' create timestamps on filenames
#' 
#' This function uses system time to timestamp file names. This was useful
#' when testing my scripts and writing out multiple versions of some of the 
#' intermediary products.
#' 
#' @param file_dir directory into which the file will be written
#' @param file_name name of the file to be written out, without a timestamp
#' 
#' @noRd
timestamp_filename <- function(file_dir, file_name){
  # Format the date and time as a string
  current_time <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  
  # Create the new file name with the timestamp
  new_file_name <- paste0(file_dir, file_name, 
                          "_", current_time, ".csv") |>
    str_replace_all(" ", "_")
  
  return(new_file_name)
}


#' Get a file from the Research Workspace
#' 
#' This function uses uses the file_id, file_name, and RW url formatting 
#' to create a wget command for downloading the specified file into 
#' a folder named based on the NPRB project code (YYNN), where YY is
#' the year the project award was distributed and NN is the sequential project 
#' number assigned by NPRB
#' 
#' @param p_code the project code assigned by NPRB for the project that 
#' created the data
#' @param f_id the file_id from the RW DB for the file to be downloaded
#' @param f_name the name of the file in the RW
#' 
#' @noRd
wget_file_from_rw <- function(out_dir, f_id, f_name){
  # given a the id and name of a file in the RW, pull the file down
  # mkdir of project code, e.g. 0204, 0908, etc.
  file_url <- paste0("https://researchworkspace.com/files/",
           as.character(f_id), "/", f_name)
  api_key <- "zMmzunPWHV68Vg"
  wget_command <- paste0('wget --header "api-key: ', api_key,
                        '" -P ', out_dir, ' ', file_url)
  system(wget_command)
  #download.file(file_url, local_file, mode = "wb")  
}




# '" -P /data/',out_dir,
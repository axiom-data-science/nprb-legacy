

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

get_next_gen <- function(folders_df){
  cloned_df <- folders_df
  folders_df <- left_join(folders_df, cloned_df, by = c("parent_id" = "folder_id"),
                          keep = FALSE, na_matches = "never", multiple = "any")
  #  print(names(folders_df))
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

get_folder_names <- function(dir_tree_ids, folders_df){
  cloned_df <- dir_tree_ids
  n <- ncol(dir_tree_ids)
  for (i in 1:n){
    w <- dir_tree_ids[,i]
    #print(names(folder_goofin)[i])
    
    folder_names <- replicate(nrow(dir_tree_ids), NA)
    names(folder_names) <- paste0("folder_names_", i)
    print(names(folder_names))
    for (j in 1:length(w)){
      if (!is.na(w[[j]])){
        folder_names[j] <- folders_df$folder_name[folders_df$folder_id == w[[j]]]
      } #else {
      #folder_names[j] <- "no_folder"
      #}
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

make_path <- function(x){
  for (i in 1:nrow(x)){
    path <- ""
    for (j in 1:ncol(x)){
      if (!is.na(x[i,j]) && !is.numeric(x[i,j])){
        path <- paste0(x[i,j], "/", path)
      }
    }
    x$path[i] <- path
  }
  return(x)
}
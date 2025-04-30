

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
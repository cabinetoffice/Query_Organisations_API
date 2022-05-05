first_file <- "page1.json"

folder_location <- "~/Codes/QueryOrganisationsAPI/outputs/raw_pages"

first_file_name <- paste(folder_location, first_file, sep = "/")

curl::curl_download("https://www.gov.uk/api/organisations", first_file_name)

first_page <- jsonlite::read_json(first_file_name)

num_pages <- first_page$pages

downloadPage <- function(page_num){
  
  query = paste0("https://www.gov.uk/api/organisations?page=", page_num)
  
  filename = paste0(folder_location, "/page", page_num, ".json")
  
  curl::curl_download(query, filename)
  
}

2:num_pages %>% 
  purrr::map(
    downloadPage  
  )

readResults <- function(page_num){
  
  filename = paste0(folder_location, "/page", page_num, ".json")
  
  data <- jsonlite::read_json(filename, simplifyVector = T) 
  
  data$results %>%
    dplyr::rowwise() %>% 
    ## Only take the first parent organisation.
    dplyr::mutate(
      parent_org_ids = stringr::str_c(parent_organisations$id, collapse = ";"),
      num_parent_orgs = 1 + stringr::str_count(parent_org_ids, ";")
    )
}

results <- 
  1:num_pages %>% 
  purrr::map_dfr(
    readResults
  ) %>% 
  jsonlite::flatten() %>% 
  dplyr::mutate(row = dplyr::row_number())

parent_org_details_lookup <- 
  results

## Note that this gives a different total to the amount displayed on govuk.
results_not_closed <- 
  results %>% 
  dplyr::filter(is.na(details.govuk_closed_status)) %>% 
  dplyr::left_join(parent_org_details_lookup, by = c("parent_org_ids" = "id"), suffix = c("", "_parent")) %>% 
  dplyr::select(where(is.atomic))

folder_location_inputs <- "~/Codes/QueryOrganisationsAPI/inputs"

folder_location_outputs <- "~/Codes/QueryOrganisationsAPI/outputs"

readr::write_csv(results_not_closed, file = paste(folder_location_inputs, "orgs_not_closed_raw.csv", sep = "/"))

parent_orgs_manually_allocated_for_multiple_parents <- readr::read_csv("~/Codes/QueryOrganisationsAPI/inputs/manually_allocated_parent_for_multiple_parent_bodies.csv")

results_live_non_devolved <- 
  results_not_closed %>% 
  dplyr::filter(format == "Devolved administration")

results_manual_allocated_for_multiple_parents <- 
  results_not_closed %>% 
  dplyr::left_join(parent_orgs_manually_allocated_for_multiple_parents)

allowed_top_level_parent_org_slugs <- 
  readr::read_csv(paste(folder_location_inputs, "top_level_parent_orgs.csv", sep = "/"))

allowed_top_level_parent_org_ids <- 
  allowed_top_level_parent_org_slugs %>% 
  dplyr::left_join(results_not_closed, by = c("top_level_parent_orgs_slug" = "details.slug")) %>% 
  dplyr::pull(id)

results_allowed_top_level_parent <- 
  results_manual_allocated_for_multiple_parents %>% 
  dplyr::mutate(
    top_level_parent_org_in_original_data = dplyr::case_when(
      parent_org_ids %in% allowed_top_level_parent_org_ids ~ T,
      T ~ F
    )
  ) %>% 
  tibble::as_tibble()

findTopLevelOrgRecursive <- function(parent_id, lookup_df = results_allowed_top_level_parent){
  num_ids = 1 + stringr::str_count(parent_id, ";")

  ## If no parent ID is provided, return NA. Also returns NA for situations where
  ## the parent ID is not actually an ID in the ID column.
  if(is.na(parent_id) | stringr::str_squish(parent_id) %>% stringr::str_trim() == ""){
    NA_character_
  } else if(num_ids > 1) {
    ## Else if there are multiple IDs present, return NA (we will use some 
    # manual allocations later)
    NA_character_
  } else if(parent_id %in% allowed_top_level_parent_org_ids) {
    parent_id
  } else {
    ## Else look for the parent of the parent. 

    parents_parent_id <- lookup_df %>% 
      dplyr::filter(id == parent_id) %>% 
      dplyr::pull(parent_org_ids) 
    
    ## If the parent_id doesn't exist in the id column, it will return a
    ## zero-length vector. Therefore we instantiate a length-1 empty vector
    ## here.
    if(length(parents_parent_id) == 0){
      parents_parent_id = ""
    }
    
    findTopLevelOrgRecursive(parents_parent_id)
  }
}

results_top_level_parent_recursive_or_multiple <- 
  results_allowed_top_level_parent %>% 
  dplyr::rowwise() %>%
  dplyr::mutate(
    top_level_parent_recursive_search = findTopLevelOrgRecursive(parent_org_ids)
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(
    top_level_parent_recursive_or_multiples = dplyr::case_when(
      !is.na(top_level_parent_recursive_search) ~ top_level_parent_recursive_search,
      num_parent_orgs > 1 ~ parent_org_manual_duplicates
    )
  )

## This is used to assign top-level parents manually.
missing_top_level_parents <- 
  results_top_level_parent_recursive_or_multiple %>% 
  dplyr::filter(is.na(top_level_parent_recursive_or_multiples) & !(id %in% allowed_top_level_parent_org_ids)) %>% 
  dplyr::mutate(org_with_missing_parent = dplyr::case_when(
    is.na(parent_org_ids) | parent_org_ids == "" ~ id,
    T ~ parent_org_ids
  )) %>% 
  dplyr::select(
    org_with_missing_parent
  ) %>% 
  dplyr::filter(org_with_missing_parent != "") %>% 
  dplyr::distinct()

readr::write_excel_csv(missing_top_level_parents, paste(folder_location_outputs, "no_valid_top_level_org.csv", sep = "/"))

manual_allocations_single_parent_orgs <- 
  readr::read_csv("~/Codes/QueryOrganisationsAPI/inputs/manual_allocated_parent_orgs_single.csv") %>% 
  dplyr::rename(
    id = "org_with_missing_parent",
    manual_allocated_parent_single = "manual_allocated_parent"
    )

manual_parents <- 
  readr::read_csv(paste(folder_location_inputs, "manual_parents.csv", sep = "/"))

results_add_manual_singles <- 
  results_top_level_parent_recursive_or_multiple %>% 
  dplyr::left_join(manual_parents) %>% 
  dplyr::left_join(
    manual_allocations_single_parent_orgs,
    by = "id"
  ) %>% 
  dplyr::left_join(
    manual_allocations_single_parent_orgs,
    by = c("parent_org_ids" = "id")
  ) %>% 
  dplyr::mutate(
    manual_allocated_parent_single_final = dplyr::case_when(
      !is.na(manual_allocated_parent_single.x) ~ manual_allocated_parent_single.x,
      !is.na(manual_allocated_parent_single.y) ~ manual_allocated_parent_single.y,
      T ~ NA_character_
    )
  )

results_compiled <- 
  results_add_manual_singles %>% 
  dplyr::rowwise() %>% 
  dplyr::mutate(
    manual_top_level_single = findTopLevelOrgRecursive(manual_allocated_parent_single_final)
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(
    top_level_parent_org_final = dplyr::case_when(
      !is.na(top_level_parent_recursive_or_multiples) ~ top_level_parent_recursive_or_multiples,
      !is.na(manual_top_level_single) ~ manual_top_level_single,
      T ~ NA_character_
    )
  ) %>% 
  dplyr::mutate(
    parent_id_final = dplyr::case_when(
      id %in% allowed_top_level_parent_org_ids ~ id,
      !is.na(manual_parent_id) ~ manual_parent_id,
      parent_org_ids == "" | is.na(parent_org_ids) ~ top_level_parent_org_final,
      num_parent_orgs > 1 ~ parent_org_manual_duplicates,
      T ~ parent_org_ids
    )
  ) %>% 
  dplyr::rowwise() %>%
  dplyr::mutate(
    details.abbreviation = dplyr::case_when(
      is.na(details.abbreviation) ~ stringr::str_split(details.slug, pattern = "-") %>%
        unlist() %>%
        stringr::str_extract_all(pattern = '^.') %>%
        stringr::str_flatten() %>%
        stringr::str_to_upper(),
      ## This handles the case of Soane's, where the abbreviation contains a
      ## comma which breaks DiagrammeR.
      T ~ details.abbreviation %>% stringr::str_replace_all(pattern = "'", replacement = "")
    )
  ) %>% 
  dplyr::ungroup()

parent_lookup <- 
  results_compiled %>% 
  dplyr::select(
    row_parent_final = "row",
    id_parent_final = "id",
    title_parent_final = "title",
    details.slug_parent_final = details.slug,
    details.abbreviation_parent_final = details.abbreviation
  )

results_final <- 
  results_compiled %>% 
  dplyr::left_join(
    parent_lookup, by = c("parent_id_final" = "id_parent_final")
  )

top_level_lookup <- 
  results_final %>% 
  dplyr::filter(id %in% allowed_top_level_parent_org_ids) %>% 
  dplyr::select(
    row_top_level = "row",
    id_top_level = "id", 
    title_top_level = "title", 
    slug_top_level = "details.slug",
    abbreviation_top_level = "details.abbreviation"
  )

org_list_for_docs <- 
  results_final %>%
  dplyr::left_join(top_level_lookup, by = c("top_level_parent_org_final" = "id_top_level")) %>%
  dplyr::select(
    `Organisation` = title,
    `ID (API)` = id,
    `Slug (readable ID)` = details.slug,
    `Abbreviation` = details.abbreviation,
    `Top-level sponsor organisation` = title_top_level,
    `Top-level sponsor organisation ID (API)` = top_level_parent_org_final,
    `Top-level sponsor organisation slug (readable ID)` = slug_top_level,
    `Top-level sponsor organisation abbreviation` = abbreviation_top_level
  ) %>%
  dplyr::mutate(
    `Top-level sponsor organisation abbreviation` = dplyr::case_when(
      `Top-level sponsor organisation slug (readable ID)` == "cabinet-office" ~ "CO",
      `Top-level sponsor organisation slug (readable ID)` == "office-of-the-secretary-of-state-for-scotland" ~ "OSSS",
      T ~ `Top-level sponsor organisation abbreviation`
    )
  ) %>% 
  dplyr::filter(
    # !(`ID (API)` %in% allowed_top_level_parent_org_ids),
    !(`Top-level sponsor organisation ID (API)` %in% c("https://www.gov.uk/api/organisations/welsh-government",
                                                       "https://www.gov.uk/api/organisations/northern-ireland-executive",
                                                       "https://www.gov.uk/api/organisations/the-scottish-government"))
    )%>% 
  ## Create a 3-digit organisation code. Having all organisation codes be 3 
  ## digits should make it less likely to cause confusion.
  dplyr::mutate(`Organisation Code (GCS Data Audit)` = 100 + dplyr::row_number()) %>% 
  dplyr::relocate(`Organisation Code (GCS Data Audit)`, .before = `Organisation`)

sysdatetime <- Sys.time() %>% 
  format("%Y-%m-%d_%H-%M-%S_%Z")

wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "Organisations Table")
openxlsx::writeDataTable(wb, sheet = 1, org_list_for_docs)
openxlsx::saveWorkbook(wb, paste0(folder_location_outputs, "/", sysdatetime, " Organisations list for DoCs.xlsx"))

results_final_trimmed <- 
  results_final %>% 
  dplyr::select(
    row,
    id,
    title,
    format,
    details.slug,
    details.abbreviation,
    row_parent_final,
    id_parent_final = "parent_id_final",
    num_parent_orgs,
    title_parent_final,
    details.slug_parent_final,
    details.abbreviation_parent_final,
    top_level_parent_org_final
  )

getSlugFromID <- function(id){
  stringr::str_split(id, pattern = "/") %>% 
    unlist() %>% 
    dplyr::last()
}

makeGraph <- function(filter_id){
  
  slug = getSlugFromID(filter_id)
  
  print(slug)
  
  results_for_graph <- 
    results_final_trimmed %>% 
    dplyr::filter(id == filter_id | top_level_parent_org_final == filter_id)
  
  nodes <- 
    DiagrammeR::create_node_df(
      n = length(results_for_graph$details.abbreviation),
      label = results_for_graph$id
    )
  
  results_with_edges <- 
    results_for_graph %>% 
    dplyr::left_join(nodes, by = c("id" = "label"), suffix = c("", "_node")) %>% 
    dplyr::left_join(nodes, by = c("id_parent_final" = "label"), suffix = c("", "_node_parent"))
  
  nodes_2 <- 
    DiagrammeR::create_node_df(
      n = length(results_for_graph$details.abbreviation),
      label = results_for_graph$details.abbreviation
    )
  
  edges <-
      DiagrammeR::create_edge_df(
        from = results_with_edges$id_node,
        to = results_with_edges$id_node_parent
      ) %>% 
    dplyr::filter(from != to)

  assign("re", results_with_edges, .GlobalEnv)
  
  graph <- DiagrammeR::create_graph(nodes_df = nodes_2, edges_df = edges) %>% 
    DiagrammeR::export_graph(paste0(folder_location_outputs, "/", sysdatetime, " ", slug, ".png"))

}

for(id in allowed_top_level_parent_org_ids){
  try(makeGraph(id))
}

nodes <- 
  DiagrammeR::create_node_df(
    n = length(results_final_trimmed$details.abbreviation),
    label = results_final_trimmed$id
  )

results_with_edges <- 
  results_final_trimmed %>% 
  dplyr::left_join(nodes, by = c("id" = "label"), suffix = c("", "_node")) %>% 
  dplyr::left_join(nodes, by = c("id_parent_final" = "label"), suffix = c("", "_node_parent"))

nodes_2 <- 
  DiagrammeR::create_node_df(
    n = length(results_final_trimmed$details.abbreviation),
    label = results_final_trimmed$details.abbreviation
  )

edges <-
  DiagrammeR::create_edge_df(
    from = results_with_edges$id_node,
    to = results_with_edges$id_node_parent
  ) %>% 
  dplyr::filter(from != to)

graph <- DiagrammeR::create_graph(nodes_df = nodes_2, edges_df = edges)

DiagrammeR::export_graph(graph, paste0(folder_location_outputs, "/", sysdatetime, " All Orgs.png"))

# Loads the rvest, stringr and dplyr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
    library(glue)
  })
})

# Language: us-english
Sys.setlocale("LC_ALL", "en_US.UTF-8")

setwd('C:\\Users\\joaov\\Documents\\Cask Tracker')

# Defines the first page URL
first_page_url <- 'https://whiskyauctioneer.com/auction-search?text=cask&sort=field_reference_field_end_date+DESC&items_per_page=250&f%5B0%5D=bottle_size%3A65'

# Loads the first page
first_page <- read_html(first_page_url)

content <- first_page %>%
  html_nodes("div.view-content") %>%
  html_nodes('div')
content <- content[grep("views-row", content)]

lots_data <- data.frame()

base <- read.csv2("C:\\Users\\joaov\\Documents\\Cask Tracker\\WhiskyAuctioneer - Informations.csv", sep=';')

for (i in 1:length(content)){
  
  lot_link <- content[i] %>%
    html_nodes('a') %>%
    html_attr('href') %>%
    trimws()
  lot_link <- lot_link[grep("^https", lot_link)]
  
  if (lot_link %in% base$link){
    next
  }
  
  price <- content[i] %>%
    html_nodes('span.WinningBid') %>%
    html_nodes('span.uc-price') %>%
    html_text() %>%
    gsub('£|,','',.) %>%
    as.numeric()
  
  sold <- content[i] %>%
    html_nodes('span.WinningBid') %>%
    html_nodes('div.reserve-price') %>%
    html_text() %>%
    regmatches(., gregexpr("\\(([^)]+)\\)", .))
  
  if (length(sold) == 0) {sold <- NA} else {sold <- gsub('\\(|\\)','',sold[[1]])}
  
  auction_end <- content[i] %>%
    html_nodes('div.enddatein') %>%
    html_nodes('span.uc-price') %>%
    html_text() %>%
    gsub("\\.", "/", .) %>%
    as.Date(format = '%d/%m/%y')
   #gsub(".*:", "", .)
  
  if (length(auction_end) == 0) {auction_end <- 'Unfinished Auction'}
  
  title <- content[i] %>%
    html_nodes('a') %>%
    html_attr('_title') %>%
    .[1]
  
  lot_data = data.frame(link=lot_link,
                        price=price,
                        sold=sold,
                        auction_end=auction_end,
                        title=title)
  
  lots_data <- bind_rows(lots_data, lot_data)
  
  Sys.sleep(30)
  
}

mapping <- list('Distillery'='distillery',
                'Vintage'='year_ditilled',
                'Region'='region',
                'Bottler'='bottler',
                'Cask Type'='cask_type',
                'Bottled Strength'='strength',
                'Bottle Size'='size',
                'Distillery Status'='distillery_status')

for (j in 1:nrow(lots_data)){
  
  second_page <- read_html(lots_data$link[j])
  
  labels <- second_page %>%
    html_nodes('div.whiskyproduct') %>%
    html_nodes('div.field-label') %>%
    html_text() %>%
    gsub(":.*", "", .)
  
  values <- second_page %>%
    html_nodes('div.whiskyproduct') %>%
    html_nodes('div.field-items') %>%
    html_text()
  
  for(k in 1:length(labels)){
    
    label <- labels[k]
    value <- values[k]
    
    if (!label %in% names(mapping)){
      print(glue("unknown info field: '{label}'"))
      mapping[[label]] <- tolower(gsub(" ", "_", label))
      lots_data[[mapping[[label]]]] <- NA
    }
  
    lots_data[[mapping[[label]]]][j] <- value
  
  }
  
  details <- second_page %>%
    html_nodes("div.field-items") %>%
    .[[length(.)]] %>%
    html_text()

  lots_data[['details']][j] <- details
  
  image <- second_page %>%
    html_node("div.slick-zoom") %>%
    html_attr('data-src')
  
  lots_data[['image']][j] <- image
  
  Sys.sleep(30)
  print(j)
  
}

base$auction_end <- as.Date(base$auction_end, format = "%d/%m/%Y")

base <- base %>% mutate(across(everything(), as.character))
lots_data <- lots_data %>% mutate(across(everything(), as.character))

lots_data <- bind_rows(base, lots_data)

write.csv2(lots_data, file = "WhiskyAuctioneer - Informations.csv", row.names = FALSE)

# Import necessary libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(dplyr)
    library(rvest)
    library(stringr)
    library(glue)
  })
})

# Language: us-english
Sys.setlocale("LC_ALL", "en_US.UTF-8")

setwd('C:\\Users\\joaov\\Documents\\Whisky Casks ETL')
  
# Defines the The Grand whisky Auctions URL
url <- "https://www.thegrandwhiskyauction.com/past-auctions/cask/180-per-page"

# Scrape the auction data
casks_data <- 
  # Read HTML content from the specified URL
  read_html(url) %>%
  # Extract the script element containing auction data using XPath 'auction_data ='
  html_element(xpath = "//script[contains(text(),'auction_data = ')]") %>%
  # Extract text content from the script element
  html_text() %>%
  # Clean up the JavaScript to make it parsable as JSON
  str_extract("\\[.*\\]") %>%
  # Convert the JSON-like string into a JSON list
  jsonlite::fromJSON()

# Creates the images_data data frame
images_data <- data.frame(
  lot_number = integer(0),
  url_image = character(0),
  order = integer(0))

# Map fields between characteristic labels and respective columns
mapping <- list('Distillery' = NULL,
                'Bottler' = 'bottler',
                'Country' = 'country',
                'Region' = 'region',
                'Size' = 'size',
                'Type' = 'type',
                'Age' = 'age',
                'Strength (%)' = 'strength',
                'Cask Finish' = 'cask_finish',
                'Distillery Status' = 'distillery_status',
                'Fill Level' = 'fill_level',
                'Lot Type' = 'lot_type',
                'Shipping Weight' = 'shipping_weight',
                'Cask No' = 'cask_no',
                'Cask Type' = 'cask_type',
                'Number of Bottles' = 'number_of_bottles')

# Adds the new columns to step2_df (all NA)
for (col in mapping){
  if (!is.null(col)) {
    casks_data[[col]] <- NA
  }
}

casks_data <- casks_data %>%
  mutate(description = NA)

for (i in 1:nrow(casks_data)){
  
  cask_url <- casks_data$url[i]
  cask_page <- read_html(cask_url)
  
  casks_data$description[i] <- cask_page %>% html_nodes('.innerText p') %>% html_text() %>% str_trim() %>% paste(collapse = " ")
  
  lot_information <- cask_page %>% html_nodes('.lotProps li') %>% html_text() %>% str_trim()
  lot_information <- lot_information[2:length(lot_information)]
  # Goes through all lines
  for (information in lot_information){
    # Extracts line label
    label <- information %>% str_split(':') %>% unlist() %>% .[1] %>% str_trim()
    # Extracts line value
    value <- information %>% str_split(':') %>% unlist() %>% .[2] %>% str_trim()
    # Checks if the label exists in the mapping list (if not, stops and throws an error)
    if (!label %in% names(mapping)) stop(glue("unknown info field: '{label}'"))
    # Checks if the label is relevant
    if (is.null(mapping[[label]])) next
    # Assigns the value to the corresponding column and row in the Step 2 data frame
    casks_data[[mapping[[label]]]][i] <- value
  }
  
  Sys.sleep(30)
  print(i)
  
}

# Exports a RDS file
write.csv2(casks_data, file = "The Grand Whisky Auction - Database.csv", row.names = FALSE)
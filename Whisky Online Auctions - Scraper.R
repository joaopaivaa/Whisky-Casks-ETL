# Loads the rvest, stringr, dplyr, glue and purrr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
    library(glue)
    library(purrr)
    library(lubridate)
  })
})

# Language: us-english
Sys.setlocale("LC_ALL", "en_US.UTF-8")

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL")

# Loads the auction page
first_page <- read_html('https://live.whisky-onlineauctions.com/pages/search-results?sortBysearch=&term=held&scope=previous&show_products=96&sort_by=high-low')
# Extracts the lots sections
lots <- first_page %>% html_nodes('#productCollectionsallSerachMaster') %>% html_nodes('div.container') %>% .[2:length(.)]

lots_data = data.frame()

# Goes through all lots
for (lot in lots){
  # Extracts lot link
  link <- lot %>% html_nodes('a') %>% html_attr("href") %>% .[1]
  # Extracts lot title
  title <- lot %>% html_nodes('h5.auction-highlight__title') %>% html_text() %>% str_trim()
  # Extracts lot hammer price
  hammer_price <- lot %>% html_nodes('div.auction-highlight__bid-details') %>% html_nodes('p') %>% html_text() %>% str_replace_all(., '[£,]', '') %>% as.numeric(.)
  # Buyer price = hammer price + 15% comission (Buyers do not have to pay UK VAT)
  buyer_price <- round(hammer_price * 1.15, 2)
  # Creates lot data frame
  lot_df <- data.frame(title = title,
                       buyer_price = buyer_price,
                       hammer_price = hammer_price,
                       link = link)
  # Saves each lot values into Step 2 data frame
  lots_data = rbind(lots_data, lot_df)
}

lots_data <- lots_data %>%
  mutate(id = NA,
         status = NA,
         sold = NA,
         end_date = NA,
         description = NA,
         filling_date = NA,
         cask_number = NA,
         distillery = NA,
         year_distilled = NA,
         age = NA,
         cask_type = NA,
         original_litres_alcohol = NA,
         original_bulk_litres = NA,
         regauged_litres_alcohol = NA,
         regauged_bulk_litres = NA,
         strength = NA)

for (j in 1:nrow(lots_data)){
  # Loads second page
  second_page <- read_html(lots_data$link[j])
  # Extracts lot informations from second page
  lot_infos <- second_page %>% html_nodes('#view-all-lots')
  # Extracts lot ID
  id <- lot_infos %>% html_nodes('span.product_main_iot_44') %>% html_text() %>% strsplit(':') %>% unlist() %>% .[2] %>% str_trim()
  lots_data[['id']][j] <- id
  # Extracts lot status
  status <- lot_infos %>% html_nodes('span.auction-highlight__bid-label') %>% html_text()
  lots_data[['status']][j] <- status
  sold <- ifelse(status=='Winning Bid', TRUE, FALSE)
  lots_data[['sold']][j] <- sold
  # Extracts end date
  end_date <- lot_infos %>% html_nodes('#product-div') %>% html_nodes('span') %>% keep(~ str_detect(html_text(.), "End Date:")) %>% html_text() %>% str_split(':') %>% unlist() %>% .[2] %>% dmy(.)
  lots_data[['end_date']][j] <- end_date
  # Extracts lot description and details section
  description_detais <- second_page %>% html_nodes('#description-details')
  # Extracts lot description
  description <- description_detais %>% html_nodes('div.first-para') %>% html_nodes('p') %>% html_text() %>% str_trim() %>% str_replace_all(., '\r\n', ' ') %>% paste(., collapse = " ") %>% str_trim()
  lots_data[['description']][j] <- description
  # Extracts lot details
  details <- description_detais %>% html_nodes('ul') %>% html_text() %>% str_split(., "\n\n|\n") %>% unlist(.) %>% str_trim()
  for (detail in details){
    if (str_detect(str_to_title(detail), 'Originally Filled')) {
      lots_data[['filling_date']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Cask No|Cask Number')) {
      lots_data[['cask_number']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Distillery')) {
      lots_data[['distillery']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Distilled')) {
      lots_data[['year_distilled']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Age')) {
      lots_data[['age']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Cask') & str_detect(str_to_title(detail), 'Type')) {
      lots_data[['cask_type']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Original') & str_detect(str_to_title(detail), 'Alcohol|Strength')) {
      lots_data[['original_litres_alcohol']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Original') & str_detect(str_to_title(detail), 'Bulk')) {
      lots_data[['original_bulk_litres']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Re-Gauged') & str_detect(str_to_title(detail), 'Alcohol|Strength')) {
      lots_data[['regauged_litres_alcohol']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Re-Gauged') & str_detect(str_to_title(detail), 'Bulk')) {
      lots_data[['regauged_bulk_litres']][j] <- str_to_title(detail)
    }
    if (str_detect(str_to_title(detail), 'Strength')) {
      lots_data[['strength']][j] <- str_to_title(detail)
    }
  }
}
  
# Exports the RDS file
write.csv2(lots_data, file = 'Whisky Online Auctions - Database.csv', row.names = FALSE)
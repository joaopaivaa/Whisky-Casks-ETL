# Loads the rvest, stringr and dplyr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
  })
})

# Language: us-english
Sys.setlocale("LC_ALL", "en_US.UTF-8")

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL")

search_page <- read_html("whiskyhammer_casks.html")

lots_section <- search_page %>% html_nodes('div.itemsList') %>% html_nodes('div.item')

lots_df <- data.frame()

for (lot in lots_section) {
  
  lot_title <- lot %>% html_nodes('a.itemTitle') %>% html_text()
  
  lot_url <- lot %>% html_nodes('a.itemTitle') %>% html_attr('href')
  
  lot_auction_date <- lot %>% html_nodes('div.itemPriceWrap span.current') %>% html_text() %>% str_replace('Sold', '') %>% trimws()
  
  lot_price <- lot %>% html_nodes('div.itemPriceWrap span.priceStandard span.multiprice span.GBP.show') %>% html_text() %>% str_replace('£', '') %>% str_replace(',', '') %>% trimws()
  
  lot_image <- lot %>% html_nodes('div.itemImageWrap a img.js_lazy') %>% html_attr('data-src') %>% paste0('https://www.whiskyhammer.com', .)
  
  lot_df <- data.frame(lot_title = lot_title,
                       lot_url = lot_url,
                       lot_auction_date = lot_auction_date,
                       lot_price = lot_price,
                       lot_image = lot_image)
  
  lots_df <- bind_rows(lots_df, lot_df)
  
}

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\bronze")

write.csv2(lots_df, 'Whisky Hammer - Casks Database.csv')

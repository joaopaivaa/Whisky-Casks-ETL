# Loads the rvest, stringr and dplyr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
    library(glue)
    library(lubridate)
    library(httr)
    library(jsonlite)
  })
})

Sys.setlocale(category = "LC_TIME", locale = "en_US.UTF-8")

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\silver")

# Whisky Auctioneer

df_whisky_auctioneer <- read.csv2("whisky_auctioneer.csv", sep=',')
sort(colnames(df_whisky_auctioneer))

df_whisky_auctioneer <- df_whisky_auctioneer %>%
  mutate(
    age = as.character(age)
  )

# The Grand Whisky Auction

df_the_grand <- read.csv2("the_grand_whisky_auction.csv", sep=',')
sort(colnames(df_the_grand))

# Just Whisky Auctions

df_just_whisky <- read.csv2("just_whisky_auctions.csv", sep=',', )
sort(colnames(df_just_whisky))

# Prestige Whisky Auction

df_prestige_whisky <- read.csv2("prestige_whisky_auction.csv", sep=',')
sort(colnames(df_prestige_whisky))

# Whisky Online Auctions

df_whisky_online <- read.csv2("whisky_online_auctions.csv", sep=',')
sort(colnames(df_whisky_online))

# Whisky Hammer

df_whisky_hammer <- read.csv2("whisky_hammer.csv", sep=',')
sort(colnames(df_whisky_hammer))

# Join

df <- bind_rows(df_whisky_auctioneer, df_the_grand, df_just_whisky, df_prestige_whisky, df_whisky_online, df_whisky_hammer)

sort(colnames(df))

# Monthly volume

df_months_years <- data.frame(
  data = seq.Date(
    from = min(as.Date(df$auction_date) %m-% months(12)),
    to   = max(as.Date(df$auction_date)),
    by   = "month"
  )
) %>%
  mutate(
    year = year(data),
    month = month(data)
  ) %>%
  select(year, month)

df <- df %>%
  mutate(
    month = month(auction_date),
    year = year(auction_date)
  )

df_monthly_volume <- df %>% group_by(year, month) %>% count()

df_monthly_volume <- df_monthly_volume %>%
  right_join(df_months_years) %>%
  rename(
    volume = n
  ) %>%
  mutate(
    volume = if_else(is.na(volume), 0, volume)
  )

df_monthly_volume <- df_monthly_volume %>% 
  arrange(year, month) %>%
  mutate(
    volume = as.numeric(volume),
  )

df_monthly_volume$volume_12m = NA

for (i in seq(12:nrow(df_monthly_volume))){
  
  i_12 = i + 12
  
  df_monthly_volume[i_12, 'volume_12m'] = df_monthly_volume[(i):(i_12-1), 'volume'] %>% sum()
  
}

for (i in seq(6:(nrow(df_monthly_volume)-1))){
  
  i_6 = i + 6
  
  df_monthly_volume[i_6, 'volume_6m'] = df_monthly_volume[(i):(i_6-1), 'volume'] %>% sum()
  
}

for (i in seq(3:(nrow(df_monthly_volume)-1))){
  
  i_3 = i + 3
  
  df_monthly_volume[i_3, 'volume_3m'] = df_monthly_volume[(i):(i_3-1), 'volume'] %>% sum()
  
}

df_monthly_volume <- df_monthly_volume %>%
  select(-volume)

# Inflation adjustment

most_recent_date <- df %>%
  summarise(max_date = max(auction_date)) %>%
  pull(max_date) %>%
  as.character() %>%
  substr(1, 10) %>%
  gsub("-", "/", .)

list_of_dates <- df$auction_date
list_of_values <- df$hammer_price

dates_as_strings <- list_of_dates %>%
  as.character() %>%
  substr(1, 10) %>%
  gsub("-", "/", .)

payload <- list(
  dates = dates_as_strings,
  values = as.list(list_of_values),
  currency = "GBP",
  present_date = most_recent_date
)

headers <- add_headers(
  "Content-Type" = "application/json"
)

response <- POST(
  url = "https://financial-utilities-api.onrender.com/inflation_adjustment",
  body = payload,
  encode = "json",
  headers
)

if (status_code(response) != 200) {
  
  Sys.sleep(60)
  
  response <- POST(
    url = "https://financial-utilities-api.onrender.com/inflation_adjustment",
    body = payload,
    encode = "json",
    headers
  )
  
  if (status_code(response) != 200) {
    
    Sys.sleep(60)
    
    response <- POST(
      url = "https://financial-utilities-api.onrender.com/inflation_adjustment",
      body = payload,
      encode = "json",
      headers
    )
    
    if (status_code(response) != 200) {
    
      stop(
        paste(
          "API error",
          status_code(response),
          content(response, "text", encoding = "UTF-8")
        )
      )
      
    }
    
  }
  
}

df_inflation_adjusted_values <- content(
  response,
  as = "parsed",
  simplifyVector = FALSE
)

df_inflation_adjusted_values <- data.frame(
  date = as.character(as.Date(unlist(df_inflation_adjusted_values$Date))),
  original_value = as.numeric(unlist(df_inflation_adjusted_values$Value)),
  inflation_adjusted_hammer_price = as.numeric(unlist(df_inflation_adjusted_values[["Adjusted Value"]])),
  acumulated_inflation_rate = as.numeric(unlist(df_inflation_adjusted_values[["Accumulated Inflation (%)"]]))
)

df$inf_adj_hammer_price <- df_inflation_adjusted_values$inflation_adjusted_hammer_price

# Remove extra spaces

df <- df %>%
  mutate(
    auction_house = trimws(gsub("\u00A0", "", auction_house)),
    cask_filling = trimws(gsub("\u00A0", "", cask_filling)),
    cask_type = trimws(gsub("\u00A0", "", cask_type)),
    country = trimws(gsub("\u00A0", "", country)),
    currency = trimws(gsub("\u00A0", "", currency)),
    distillery = trimws(gsub("\u00A0", "", distillery)),
    distillery_status = trimws(gsub("\u00A0", "", distillery_status)),
    previous_spirit = trimws(gsub("\u00A0", "", previous_spirit)),
    region = trimws(gsub("\u00A0", "", region)),
    title = trimws(gsub("\u00A0", "", title))
  )

# Set variables types

df <- df %>%
  mutate(
    inf_adj_hammer_price = as.numeric(inf_adj_hammer_price),
    age = as.numeric(age),
    rla = as.numeric(rla),
    bottles_at_cask_strength = as.numeric(bottles_at_cask_strength)
  )

# New variables

df <- df %>%
  mutate(
    inf_adj_hammer_price_per_litre_of_alcohol_times_age = inf_adj_hammer_price * age / rla,
    inf_adj_hammer_price_per_bottle_at_cask_strength = round(inf_adj_hammer_price / bottles_at_cask_strength, 2),
    inf_adj_hammer_price_per_litre_of_alcohol = round(inf_adj_hammer_price / rla, 2),
  )

df <- df %>% left_join(df_monthly_volume)

# Save full database

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\gold")

write.csv(df, file = "casks_database.csv", row.names = FALSE, fileEncoding = "UTF-8")

# Rows selection

df <- df %>%
  filter(
    !is.na(age) & !is.na(previous_spirit) & !is.na(rla)
  )

# Select columns

df <- df %>%
  select(
    auction_date, distillery, region, country, strength, rla, bulk_litres, distillery_status,
    cask_type, cask_filling, previous_spirit, age, bottles_at_cask_strength, volume_12m, volume_6m, volume_3m,
    inf_adj_hammer_price, inf_adj_hammer_price_per_bottle_at_cask_strength, inf_adj_hammer_price_per_litre_of_alcohol,
    inf_adj_hammer_price_per_litre_of_alcohol_times_age
  )

# Save database for casks valuation ML model

write.csv(df, file = "casks_database__casks_valuation.csv", row.names = FALSE, fileEncoding = "UTF-8")

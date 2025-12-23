# Loads the rvest, stringr and dplyr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
    library(glue)
  })
})

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\bronze")

df_just_whisky <- read.csv2("Just Whisky Auctions - Casks Database.csv", sep=';')

df_just_whisky <- df_just_whisky %>%
  rename(
    'bulk_litres' = 'regauge_bulk_litres',
    'rla' = 'regauge_rla',
    'strength' = 'regauge_strength'
  )

df_just_whisky <- df_just_whisky %>%
  mutate(
    strength = str_replace(strength, '%', '')
  )

df_just_whisky <- df_just_whisky %>%
  mutate(
    filling_date = if_else(
      nchar(filling_date) == 4,
      paste0("01/01/", filling_date),
      filling_date
    )
  )

df_just_whisky <- df_just_whisky %>%
  mutate(
    rla = as.numeric(rla),
    strength = as.numeric(strength),
    bulk_litres = as.numeric(bulk_litres),
    bottles_at_cask_strength = as.numeric(bottles_at_cask_strength),
    auction_date = as.Date(auction_date, format='%d/%m/%Y'),
    filling_date = as.Date(filling_date, format='%d/%m/%Y'),
    regauged_date = as.Date(regauged_date, format='%d/%m/%Y')
  )

for (line in 1:nrow(df_just_whisky)){
  
  if (is.na(df_just_whisky$rla[line]) & !is.na(df_just_whisky$bulk_litres[line]) & !is.na(df_just_whisky$strength[line])){
    df_just_whisky$rla[line] = round((df_just_whisky$strength[line]/100) * df_just_whisky$bulk_litres[line], 2)
  }
  
  else if (!is.na(df_just_whisky$rla[line]) & is.na(df_just_whisky$bulk_litres[line]) & !is.na(df_just_whisky$strength[line])){
    df_just_whisky$bulk_litres[line] = round(df_just_whisky$rla[line] / (df_just_whisky$strength[line]/100), 2)
  }
  
  else if (!is.na(df_just_whisky$rla[line]) & !is.na(df_just_whisky$bulk_litres[line]) & is.na(df_just_whisky$strength[line])){
    df_just_whisky$strength[line] = round(100 * df_just_whisky$rla[line] / df_just_whisky$bulk_litres[line], 2)
  }
  
}

df_just_whisky <- df_just_whisky %>%
  mutate(
    auction_house = 'Just Whisky Auctions',
    age = round(as.numeric((auction_date - filling_date) / 365.25), 2),
    bottles_at_cask_strength = bulk_litres / 0.7,
    hammer_price_per_bottle_at_cask_strength = round(hammer_price / bottles_at_cask_strength, 2),
    hammer_price_per_litre_of_alcohol = round(hammer_price / rla, 2),
    buyer_price = hammer_price * 0.125,
    buyer_price_per_bottle_at_cask_strength = round(buyer_price / bottles_at_cask_strength, 2),
    buyer_price_per_litre_of_alcohol = round(buyer_price / rla, 2)
  )

df_just_whisky <- df_just_whisky %>%
  select(-cask_image_link, -liquid_image_link, -lot, -original_rla, -original_strength, -original_bulk_litres)

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\silver")

write.csv(df_just_whisky, file = "just_whisky_auctions.csv", row.names = FALSE, fileEncoding = "UTF-8")

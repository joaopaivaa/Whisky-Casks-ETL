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

df_prestige_whisky <- read.csv2("Prestige Whisky Auction - Casks Database.csv", sep=';')

df_prestige_whisky <- df_prestige_whisky %>%
  rename(
    'bulk_litres' = 'regauge_bulk_litres',
    'rla' = 'regauge_rla',
    'strength' = 'regauge_strength'
  )

df_prestige_whisky <- df_prestige_whisky %>%
  mutate(
    strength = str_replace(strength, '%', '')
  )

df_prestige_whisky <- df_prestige_whisky %>%
  mutate(
    age = as.integer(age),
    rla = as.numeric(rla),
    strength = as.numeric(strength),
    bulk_litres = as.numeric(bulk_litres)
  )

for (line in 1:nrow(df_prestige_whisky)){
  
  if (is.na(df_prestige_whisky$rla[line]) & !is.na(df_prestige_whisky$bulk_litres[line]) & !is.na(df_prestige_whisky$strength[line])){
    df_prestige_whisky$rla[line] = round((df_prestige_whisky$strength[line]/100) * df_prestige_whisky$bulk_litres[line], 2)
  }
  
  else if (!is.na(df_prestige_whisky$rla[line]) & is.na(df_prestige_whisky$bulk_litres[line]) & !is.na(df_prestige_whisky$strength[line])){
    df_prestige_whisky$bulk_litres[line] = round(df_prestige_whisky$rla[line] / (df_prestige_whisky$strength[line]/100), 2)
  }
  
  else if (!is.na(df_prestige_whisky$rla[line]) & !is.na(df_prestige_whisky$bulk_litres[line]) & is.na(df_prestige_whisky$strength[line])){
    df_prestige_whisky$strength[line] = round(100 * df_prestige_whisky$rla[line] / df_prestige_whisky$bulk_litres[line], 2)
  }
  
}

df_prestige_whisky <- df_prestige_whisky %>%
  mutate(
    auction_house = 'Prestige Whisky Auction',
    bottles_at_cask_strength = round(as.numeric(bulk_litres) / 0.7, 2),
    hammer_price_per_bottle_at_cask_strength = round(hammer_price / bottles_at_cask_strength, 2),
    hammer_price_per_litre_of_alcohol = round(hammer_price / rla, 2),
    buyer_price = hammer_price * 0.1,
    buyer_price_per_bottle_at_cask_strength = round(buyer_price / bottles_at_cask_strength, 2),
    buyer_price_per_litre_of_alcohol = round(buyer_price / rla, 2),
  )

df_prestige_whisky <- df_prestige_whisky %>%
  select(-current_storage_insurance_fee, -fee_frequency, -lot, -original_bulk_litres, -original_rla,
         -original_strength, -re_charred_cask, -registration_fee, -storage, -whisky_type)

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\silver")

write.csv(df_prestige_whisky, file = "prestige_whisky_auction.csv", row.names = FALSE, fileEncoding = "UTF-8")
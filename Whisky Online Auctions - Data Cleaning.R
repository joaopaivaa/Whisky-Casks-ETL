# Loads the rvest, stringr and dplyr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
    library(glue)
  })
})

Sys.setlocale(category = "LC_TIME", locale = "en_US.UTF-8")

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\bronze")

df_whisky_online <- read.csv2("Whisky Online Auctions - Casks Database.csv", sep=';')

df_whisky_online <- df_whisky_online %>%
  rename(
    bulk_litres = regauged_bulk_litres,
    rla = regauged_litres_alcohol,
    auction_date = end_date
  )

df_whisky_online <- df_whisky_online %>%
  mutate(
    filling_date = str_split(filling_date, 'Filled:') %>% sapply(., function(x) x[2]) %>% str_replace_all('-', '/') %>% trimws(),
    distillery = str_split(distillery, 'Distillery:') %>% sapply(., function(x) x[2]) %>% trimws(),
    cask_type = str_split(cask_type, 'Cask Type:') %>% sapply(., function(x) x[2]) %>% trimws(),
    bulk_litres = str_extract(bulk_litres, "\\d+(?:\\.\\d+)?(?=\\s*Bulk\\s*Litres)"),
    strength = str_extract(strength, "\\d+(?:\\.\\d+)?(?=%)"),
    rla = str_extract(rla, "\\d+(?:\\.\\d+)?(?=\\s*Litres Of Alcohol)")
    
  )

df_whisky_online <- df_whisky_online %>%
  mutate(
    auction_date = as.Date(auction_date),
    filling_date = as.Date(filling_date),
    auction_date = as.Date(auction_date)
  )

df_whisky_online$cask_filling <- case_when(str_detect(df_whisky_online$cask_type, "1st|First|Fresh") ~ "First Fill",
                                           str_detect(df_whisky_online$cask_type, "2nd|Second|Refill") ~ 'Second Fill',
                                           str_detect(df_whisky_online$cask_type, "3rd|Third") ~ "Third Fill",
                                           str_detect(df_whisky_online$cask_type, "Virgin|New") ~ 'Virgin Oak',
                                           TRUE ~ NA)

df_whisky_online$previous_spirit <- case_when(str_detect(df_whisky_online$cask_type, "Sherry|Oloroso|Px|Pedro Ximenez|Amontillado|Palo Cortado") ~ "Sherry",
                                              str_detect(df_whisky_online$cask_type, "Jack Daniel's|Heaven Hill|Bourbon") ~ 'Bourbon',
                                              str_detect(df_whisky_online$cask_type, "Rum|Diamond Rum|Caroni") ~ 'Rum',
                                              str_detect(df_whisky_online$cask_type, "Laphroaig") ~ 'Scotch Whisky',
                                              str_detect(df_whisky_online$cask_type, "Madeira") ~ 'Madeira',
                                              str_detect(df_whisky_online$cask_type, "Port") ~ 'Port',
                                              str_detect(df_whisky_online$cask_type, "Vin Santo|Vino Santo|Mosctael|Muscat|Moscatel|Pineau|Climens|Guthrie|Rivesaltes|Wine") ~ 'Wine',
                                              str_detect(df_whisky_online$cask_type, "Virgin") ~ 'Virgin Oak',
                                              TRUE ~ NA)

df_whisky_online$cask_type <- case_when(str_detect(df_whisky_online$cask_type, "Hogshead") ~ "Hogshead",
                                        str_detect(df_whisky_online$cask_type, "Barrel") ~ 'Barrel',
                                        str_detect(df_whisky_online$cask_type, "Butt") ~ 'Butt',
                                        str_detect(df_whisky_online$cask_type, "Quarter Cask") ~ 'Quarter Cask',
                                        str_detect(df_whisky_online$cask_type, "Barrique") ~ 'Barrique',
                                        str_detect(df_whisky_online$cask_type, "Octave") ~ 'Octave',
                                        TRUE ~ NA)



df_whisky_online <- df_whisky_online %>%
  mutate(
    rla = as.numeric(rla),
    strength = as.numeric(strength),
    bulk_litres = as.numeric(bulk_litres)
  )

for (line in 1:nrow(df_whisky_online)){
  
  if (is.na(df_whisky_online$rla[line]) & !is.na(df_whisky_online$bulk_litres[line]) & !is.na(df_whisky_online$strength[line])){
    df_whisky_online$rla[line] = round((df_whisky_online$strength[line]/100) * df_whisky_online$bulk_litres[line], 2)
  }
  
  else if (!is.na(df_whisky_online$rla[line]) & is.na(df_whisky_online$bulk_litres[line]) & !is.na(df_whisky_online$strength[line])){
    df_whisky_online$bulk_litres[line] = round(df_whisky_online$rla[line] / (df_whisky_online$strength[line]/100), 2)
  }
  
  else if (!is.na(df_whisky_online$rla[line]) & !is.na(df_whisky_online$bulk_litres[line]) & is.na(df_whisky_online$strength[line])){
    df_whisky_online$strength[line] = round(100 * df_whisky_online$rla[line] / df_whisky_online$bulk_litres[line], 2)
  }
  
}

df_whisky_online <- df_whisky_online %>%
  mutate(
    auction_house = 'Whisky Online Auctions',
    age = round(as.numeric((auction_date - filling_date) / 365.25), 2),
    bottles_at_cask_strength = round(as.numeric(bulk_litres) / 0.7, 2),
    hammer_price_per_bottle_at_cask_strength = round(hammer_price / bottles_at_cask_strength, 2),
    hammer_price_per_litre_of_alcohol = round(hammer_price / rla, 2),
    buyer_price = hammer_price * 0.1,
    buyer_price_per_bottle_at_cask_strength = round(buyer_price / bottles_at_cask_strength, 2),
    buyer_price_per_litre_of_alcohol = round(buyer_price / rla, 2),
  )
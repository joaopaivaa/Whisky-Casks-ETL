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

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\silver")

# Whisky Auctioneer

df_whisky_auctioneer <- read.csv2("whisky_auctioneer.csv", sep=',')
sort(colnames(df_whisky_auctioneer))

df_whisky_auctioneer <- df_whisky_auctioneer %>%
  mutate(bottles_at_cask_strength = as.numeric(bottles_at_cask_strength))

# The Grand Whisky Auction

df_the_grand <- read.csv2("the_grand_whisky_auction.csv", sep=',')
sort(colnames(df_the_grand))

df_the_grand <- df_the_grand %>%
  mutate(bottles_at_cask_strength = as.numeric(bottles_at_cask_strength))

# Just Whisky Auctions

df_just_whisky <- read.csv2("just_whisky_auctions.csv", sep=',', )
sort(colnames(df_just_whisky))

df_just_whisky <- df_just_whisky %>%
  mutate(
    age = as.numeric(age),
    bottles_at_cask_strength = as.numeric(bottles_at_cask_strength)
  )

# Prestige Whisky Auction

df_prestige_whisky <- read.csv2("prestige_whisky_auction.csv", sep=',')
sort(colnames(df_prestige_whisky))

df_prestige_whisky <- df_prestige_whisky %>%
  mutate(
    age = as.numeric(age),
    bottles_at_cask_strength = as.numeric(bottles_at_cask_strength)
  )

# Junção

df <- bind_rows(df_whisky_auctioneer, df_the_grand, df_just_whisky, df_prestige_whisky)

sort(colnames(df))

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\gold")

write.csv(df, file = "casks_database.csv", row.names = FALSE, fileEncoding = "UTF-8")

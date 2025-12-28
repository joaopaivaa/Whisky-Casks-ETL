# Loads the rvest, stringr and dplyr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
    library(glue)
    library(purrr)
  })
})

# Function to extract the date based on patterns
extract_date <- function(details, patterns) {
  for (pattern in patterns) {
    # Check if the pattern exists in the text
    if (grepl(pattern, details, perl = TRUE)) {
      # Extract the date using sub with the correct regex
      extracted_date <- sub(
        paste0(".*", pattern, " (\\d{1,2}\\.\\d{1,2}\\.\\d{4}|\\d{1,2}(?:st|nd|rd|th)?\\s+of\\s+[A-Za-z]+\\s+\\d{4}|\\d{1,2}/[A-Za-z]+/\\d{4}|\\d{1,2}-[A-Za-z]+-\\d{4}|\\d{1,2}/\\d{1,2}/\\d{4}|\\d{1,2}(?:st|nd|rd|th)?\\s+[A-Za-z]+\\s+\\d{4}|\\d{4}|[A-Za-z]+\\s+\\d{4}).*"),
        "\\1",
        details,
        perl = TRUE
      )
      # Return the extracted date if it differs from the original text
      if (extracted_date != details) {
        return(trimws(extracted_date)) # Trim whitespaces
      }
    }
  }
  return(NA) # Return NA if no pattern matched
}

# Função para converter datas no formato dd/mm/yyyy
convert_to_date <- function(date_string) {
  
  if (is.na(date_string)) {
    return(NA) # Se for NA, retorna NA
  }
  
  date_complete <- tryCatch(
    as.Date(date_string, format = "%d/%m/%Y"), # Formato para dia, mês e ano
    error = function(e) NA
  )
  
  if (!is.na(date_complete)) {
    return(format(date_complete, "%d/%m/%Y")) # Retorna no formato dd/mm/yyyy
  }
  
  # Primeiro tenta converter para datas completas no formato dd/mm/yyyy
  date_complete <- tryCatch(
    as.Date(date_string, format = "%d %B %Y"), # Formato para dia, mês e ano
    error = function(e) NA
  )
  
  if (!is.na(date_complete)) {
    return(format(date_complete, "%d/%m/%Y")) # Retorna no formato dd/mm/yyyy
  }
  
  # Tenta lidar com strings apenas com mês e ano
  date_complete <- tryCatch(
    as.Date(paste0("01 ", date_string), format = "%d %B %Y"),
    error = function(e) NA
  )
  
  if (!is.na(date_complete)) {
    return(format(date_complete, "%d/%m/%Y"))
  }
  
  # Tenta lidar com strings que possuem apenas o ano
  date_complete <- tryCatch(
    as.Date(paste0("01/01/", date_string), format = "%d/%m/%Y"),
    error = function(e) NA
  )
  
  if (!is.na(date_complete)) {
    return(format(date_complete, "%d/%m/%Y")) # Retorna no formato dd/mm/yyyy
  }
  
  return(NA) # Retorna NA se não conseguir converter
}

Sys.setlocale(category = "LC_TIME", locale = "en_US.UTF-8")

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\bronze")

df <- read.csv2("The Grand Whisky Auction - Casks Database.csv")

df <- df %>% select(id, url, bid_value, name, is_sold, country, region, strength, distillery_status, size, cask_type, cask_no, ends_at, type, description)

df <- rename(df, cask_info = cask_type,
                 hammer_price = bid_value,
                 sold = is_sold,
                 bulk_litres = size,
                 auction_date = ends_at,
                 title = name)

# df$age <- sub(".*?([-+]?[0-9]*\\.?[0-9]+).*", "\\1", df$age) %>%
#   as.numeric()

df$currency <- 'Great Britain Pound (£)'

# df$bottler <- str_to_title(df$bottler)
# df$bottler[df$bottler == 'N/A'] <- NA

# df$cask_info <- str_to_title(df$cask_info)

df$strength <- sub(".*?([-+]?[0-9]*\\.?[0-9]+).*", "\\1", df$strength)
# df$strength[df$strength == 'N/A'] <- NA
df$strength <- df$strength %>% as.numeric()

df$auction_end <- format(as.Date(df$auction_date, "%Y-%m-%d"), "%d/%m/%Y")

df$bulk_litres <- df$bulk_litres %>% str_remove('Litres') %>% str_remove('L') %>% str_remove('Bulk') %>% str_trim() %>% as.numeric()

df$rla <- round(df$bulk_litre * df$strength / 100, 2)

# Define patterns to search for keywords
patterns <- c(
  "(?i)Bonded On The",
  "(?i)Distilled On The",
  "(?i)Filled On The",
  "(?i)In On The",
  "(?i)Bonded In The",
  "(?i)Distilled In The",
  "(?i)Filled In The",
  "(?i)Bonded On",
  "(?i)Distilled On",
  "(?i)Filled On",
  "(?i)In On",
  "(?i)Bonded In",
  "(?i)Distilled In",
  "(?i)Filled In",
  "(?i)Bonded",
  "(?i)Distilled",
  "(?i)Filled",
  "(?i)Of A")

# Apply the function to the dataframe column
df$filling_date <- unlist(lapply(df$description, extract_date, patterns = patterns))
df$filling_date <- str_replace_all(df$filling_date, '\\.|\\-', '/')
df$filling_date <- str_replace_all(df$filling_date, 'st|nd|rd|th', '')
df$filling_date <- str_replace_all(df$filling_date, 'of ', '')

df[grepl("[a-zA-Z]", df$filling_date), 19] <- str_replace_all(df[grepl("[a-zA-Z]", df$filling_date), 19], '\\/', ' ')

df$filling_date <- unlist(lapply(df$filling_date, convert_to_date))

df$sold <- as.logical(df$sold)


df$cask_type <- case_when(str_detect(df$title, "Hogshead") ~ "Hogshead",
                          str_detect(df$title, "Barrel") ~ 'Barrel',
                          str_detect(df$title, "Butt") ~ 'Butt',
                          str_detect(df$title, "Quarter Cask") ~ 'Quarter Cask',
                          str_detect(df$title, "Barrique") ~ 'Barrique',
                          str_detect(df$title, "Octave") ~ 'Octave',
                          TRUE ~ NA)

df$cask_filling <- case_when(str_detect(df$title, "1st|First|Fresh") ~ "First Fill",
                             str_detect(df$title, "2nd|Second|Refill") ~ 'Second Fill',
                             str_detect(df$title, "Virgin") ~ 'Virgin Oak',
                             TRUE ~ NA)

df$previous_spirit <- case_when(str_detect(df$title, "Sherry|Oloroso|Px|Pedro Ximenez|Amontillado|Palo Cortado") ~ "Sherry",
                                str_detect(df$title, "Jack Daniel's|Heaven Hill|Bourbon") ~ 'Bourbon',
                                str_detect(df$title, "Rum|Diamond Rum|Caroni") ~ 'Rum',
                                str_detect(df$title, "Laphroaig") ~ 'Scotch Whisky',
                                str_detect(df$title, "Madeira") ~ 'Madeira',
                                str_detect(df$title, "Port") ~ 'Port',
                                str_detect(df$title, "Vin Santo|Vino Santo|Mosctael|Muscat|Moscatel|Pineau|Climens|Guthrie|Rivesaltes|Wine") ~ 'Wine',
                                str_detect(df$title, "Virgin") ~ 'Virgin Oak',
                                TRUE ~ NA)

# df$previous_spirit_resumed <- case_when(str_detect(df$previous_spirit, "Oloroso|Px|Amontillado|Palo Cortado") ~ "Sherry",
#                                         str_detect(df$previous_spirit, "Vin Santo|Pineau|Climens|Muscat|Guthrie|Rivesaltes|Mouton Rothschild Wine") ~ 'Wine',
#                                         TRUE ~ df$previous_spirit)

df$distillery <- df$title %>% str_split('-') %>% map_chr(1) %>% str_trim()
  
patterns <- c(
  "(?i)regauged",
  "(?i)regauged in",
  "(?i)regauged on",
  "(?i)re-gauged",
  "(?i)re-gauged in",
  "(?i)re-gauged on",
  "(?i)regauged the",
  "(?i)regauged in the",
  "(?i)regauged on the",
  "(?i)re-gauged the",
  "(?i)re-gauged in the",
  "(?i)re-gauged on the",
  "(?i)which took place in")

df$regauged_date <- unlist(lapply(df$description, extract_date, patterns = patterns))
df$regauged_date <- str_replace_all(df$regauged_date, '\\.|\\-', '/')
df$regauged_date <- str_replace_all(df$regauged_date, 'st|nd|rd|th', '')
df$regauged_date <- str_replace_all(df$regauged_date, 'of ', '')
df$regauged_date <- unlist(lapply(df$regauged_date, convert_to_date))

for (line in 1:nrow(df)){

  if (is.na(df$rla[line]) & !is.na(df$bulk_litres[line]) & !is.na(df$strength[line])){
    df$rla[line] = round((df$strength[line]/100) * df$bulk_litres[line], 2)
  }

  else if (!is.na(df$rla[line]) & is.na(df$bulk_litres[line]) & !is.na(df$strength[line])){
    df$bulk_litres[line] = round(df$rla[line] / (df$strength[line]/100), 2)
  }

  else if (!is.na(df$rla[line]) & !is.na(df$bulk_litres[line]) & is.na(df$strength[line])){
    df$strength[line] = round(100 * df$rla[line] / df$bulk_litres[line], 2)
  }

}

df$age <- round(as.numeric((as.Date(df$auction_end, format='%d/%m/%Y') - as.Date(df$filling_date, format='%d/%m/%Y')) / 365.25), 2)

# df$stored_at <- trimws(sub("(?i)distillery", "", sub(",.*", "", sub("-.*", "", sub(".*in bond by ", "", sub(".*storage at ", "", sub(".*(?i)Stored at ", "", df$title)))))))

df$bottles_at_cask_strength <- round(df$bulk_litres / 0.7, 2)

df$hammer_price_per_bottle_at_cask_strength <- round(as.numeric(df$hammer_price) / df$bottles_at_cask_strength, 2)

df$hammer_price_per_litre_of_alcohol <- round(as.numeric(df$hammer_price) / df$rla, 2)

df$buyer_price <- round(1.125 * as.numeric(df$hammer_price), 2)

df$buyer_price_per_bottle_at_cask_strength <- round(as.numeric(df$buyer_price) / df$bottles_at_cask_strength, 2)

df$buyer_price_per_litre_of_alcohol <- round(as.numeric(df$buyer_price) / df$rla, 2)

for (i in 1:nrow(df)){
  
  if (df$region[i] == 'Highlandq'){
    df$region[i] = 'Highlands'
  }
  
  if (df$region[i] %in% c('Highland', 'Lowland', 'Island')){
    df$region[i] = paste0(df$region[i], 's')
  }
  
  if ('USA' %in% df$region[i]){
    df$region[i] = 'USA'
  }
  
}

df$auction_house <- 'The Grand Whisky Auction'

df$distillery <- str_to_title(df$distillery)

df <- df %>%
  select(-cask_info, -auction_date)

df <- rename(df, auction_date = auction_end)

df <- df %>%
  mutate(
    auction_date = as.Date(auction_date, format='%d/%m/%Y'),
    filling_date = as.Date(filling_date, format='%d/%m/%Y'),
    regauged_date = as.Date(regauged_date, format='%d/%m/%Y')
  )

df <- df %>%
  mutate(
    age = as.numeric(age),
    rla = as.numeric(rla),
    bulk_litres = as.numeric(bulk_litres),
    hammer_price = as.numeric(hammer_price),
    bottles_at_cask_strength = as.numeric(bottles_at_cask_strength),
    hammer_price_per_bottle_at_cask_strength = as.numeric(hammer_price_per_bottle_at_cask_strength),
    hammer_price_per_litre_of_alcohol = as.numeric(hammer_price_per_litre_of_alcohol),
    buyer_price = as.numeric(buyer_price),
    buyer_price_per_bottle_at_cask_strength = as.numeric(buyer_price_per_bottle_at_cask_strength),
    buyer_price_per_litre_of_alcohol = as.numeric(buyer_price_per_litre_of_alcohol)
  )

# Optional/temporary
df <- df %>%
  select(-id, -cask_no, -description, -type)

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\silver")

#write.csv(df, file = "The Grand Whisky Auction - Database - Cleaned.csv", row.names = FALSE, fileEncoding = "UTF-8")
write.csv(df, file = "the_grand_whisky_auction.csv", row.names = FALSE, fileEncoding = "UTF-8")
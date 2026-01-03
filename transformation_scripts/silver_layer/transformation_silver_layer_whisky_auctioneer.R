# Loads the rvest, stringr and dplyr libraries
suppressWarnings({
  suppressPackageStartupMessages({
    library(rvest)
    library(stringr)
    library(dplyr)
    library(glue)
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

df <- read.csv2("WhiskyAuctioneer - Casks Database.csv")

df$age[df$age == 'N/A'] <- NA
df$age[df$age == 'n/a'] <- NA

df$age <- sub(".*?([-+]?[0-9]*\\.?[0-9]+).*", "\\1", df$age) %>%
  as.numeric()

df$currency <- 'Great Britain Pound (£)'

df$bottler <- str_to_title(df$bottler)
df$bottler[df$bottler == 'N/A'] <- NA

df$cask_info <- str_to_title(df$cask_info)

df$strength <- sub(".*?([-+]?[0-9]*\\.?[0-9]+).*", "\\1", df$strength)
df$strength[df$strength == 'N/A'] <- NA
df$strength <- df$strength %>% as.numeric()

df$auction_end <- format(as.Date(df$auction_end), "%d/%m/%Y")

df$size <- str_to_title(df$size)

df$bulk_litres <- ifelse(grepl("(?i)Bulk Litres", df$size), 
                               sub(".*?([-+]?[0-9]*\\.?[0-9]+).*(?i)Bulk Litres.*", "\\1", df$size),
                         
                               ifelse(grepl("(?i)Litre", df$size), 
                                      sub(".*?([-+]?[0-9]*\\.?[0-9]+).*(?i)Litre.*", "\\1", df$size), 
                         
                                      ifelse(grepl("(?i)Litres", df$size), 
                                             sub(".*?([-+]?[0-9]*\\.?[0-9]+).*(?i)Litres.*", "\\1", df$size), 
                                             
                                             NA))) %>%
  as.numeric() %>%
  round(., 2)


df$rla <- ifelse(grepl("(?i)Rla", df$size), 
                        sub(".*?([-+]?[0-9]*\\.?[0-9]+).*(?i)Rla.*", "\\1", df$size), 
                        NA)

df$rla <- ifelse(grepl("(?i)As", df$rla), 
                        sub(".*(?i)RLA.*?([-+]?[0-9]*\\.?[0-9]+).*", "\\1", df$rla), 
                        df$rla) %>%
                        as.numeric() %>%
                        round(., 2)

df$details <- gsub("(?<=\\w)(?=\\p{Lu})", " ", df$details, perl = TRUE)

df$details <- tolower(df$details)
df$details <- str_replace_all(df$details, "\\s+", " ")        # Substituir múltiplos espaços por um único
df$details <- str_trim(df$details)                            # Remover espaços no início e no fim
df$details <- str_replace_all(df$details, "(\\d{1,2})(th|st|nd|rd)", "\\1") # Remover sufixos de datas

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
  "(?i)Filled")

# Apply the function to the dataframe column
df$filling_date <- unlist(lapply(df$details, extract_date, patterns = patterns))

df$filling_date <- str_replace_all(df$filling_date, 'ferbruary', 'february')
df$filling_date <- str_replace_all(df$filling_date, '\\.|\\-', '/')
df$filling_date <- str_replace_all(df$filling_date, '(\\d+)(st|nd|rd|th)\\b', '')
df$filling_date <- str_replace_all(df$filling_date, 'of ', '')

df$filling_date <- unlist(lapply(df$filling_date, convert_to_date))

df$sold <- case_when(df$sold == "Reserve has been met" ~ TRUE,
                     df$sold == "Reserve not met" ~ FALSE)

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

# df$previous_spirit <- mapply(function(info, type) {trimws(gsub(type, "", info))}, df$cask_info, df$cask_type)
# 
# df$previous_spirit <- gsub('1st', 'First', df$previous_spirit)
# df$previous_spirit <- gsub('Fresh|New Fill', 'First Fill', df$previous_spirit)
# df$previous_spirit <- gsub('Refill', 'Second Fill', df$previous_spirit)
# df$previous_spirit <- gsub('Ex-', '', df$previous_spirit)
# df$previous_spirit <- gsub('-', ' ', df$previous_spirit)
# df$previous_spirit <- gsub('Wine', '', df$previous_spirit)
# df$previous_spirit <- gsub(' Sherry', '', df$previous_spirit)
# 
# df$previous_spirit <- replace(df$previous_spirit, df$previous_spirit == "", NA)

# df$cask_filling <- case_when(str_detect(df$previous_spirit, "First Fill") ~ "First Fill",
#                              str_detect(df$previous_spirit, "Second Fill") ~ 'Second Fill',
#                              TRUE ~ NA)

# df$previous_spirit <- trimws(gsub('First Fill|Second Fill', '', df$previous_spirit))

# df$previous_spirit <- case_when(str_detect(df$cask_info, "Oloroso") ~ "Oloroso",
#                                 str_detect(df$cask_info, "Px|PX|Pedro Ximenez") ~ "Pedro Ximenez",
#                                 str_detect(df$cask_info, "Amontillado") ~ "Amontillado",
#                                 str_detect(df$cask_info, "Palo Cortado") ~ "Palo Cortado",
#                                 str_detect(df$cask_info, "Jack Daniel's|Heaven Hill|Bourbon") ~ 'Bourbon',
#                                 str_detect(df$cask_info, "Rum|Diamond Rum|Caroni") ~ 'Rum',
#                                 str_detect(df$cask_info, "Laphroaig") ~ 'Scotch Whisky',
#                                 str_detect(df$cask_info, "Madeira") ~ 'Madeira',
#                                 str_detect(df$cask_info, "Port") ~ 'Port',
#                                 str_detect(df$cask_info, "Vino Santo|Vin Santo") ~ 'Vin Santo',
#                                 str_detect(df$cask_info, "Mosctael|Muscat|Moscatel") ~ 'Moscatel',
#                                 str_detect(df$cask_info, "Pineau") ~ 'Pineau',
#                                 str_detect(df$cask_info, "Climens") ~ 'Climens',
#                                 str_detect(df$cask_info, "Guthrie") ~ 'Guthrie',
#                                 str_detect(df$cask_info, "Rivesaltes") ~ 'Rivesaltes',
#                                 str_detect(df$cask_info, "Virgin") ~ 'Virgin Oak',
#                                 str_detect(df$cask_info, "Wine") ~ 'Wine',
#                                 TRUE ~ NA)

# df$previous_spirit_resumed <- df$previous_spirit

df$previous_spirit <- case_when(str_detect(df$title, "Sherry|Oloroso|Px|Pedro Ximenez|Amontillado|Palo Cortado") ~ "Sherry",
                                str_detect(df$title, "Jack Daniel's|Heaven Hill|Bourbon") ~ 'Bourbon',
                                str_detect(df$title, "Rum|Diamond Rum|Caroni") ~ 'Rum',
                                str_detect(df$title, "Madeira") ~ 'Madeira',
                                str_detect(df$title, "Port") ~ 'Port',
                                str_detect(df$title, "Vin Santo|Vino Santo|Mosctael|Muscat|Moscatel|Pineau|Climens|Guthrie|Rivesaltes|Wine") ~ 'Wine',
                                str_detect(df$title, "Virgin") ~ 'Virgin Oak',
                                TRUE ~ NA)

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

df$regauged_date <- unlist(lapply(df$details, extract_date, patterns = patterns))

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

df$calculated_age <- round(as.numeric((as.Date(df$auction_end, format='%d/%m/%Y') - as.Date(df$filling_date, format='%d/%m/%Y')) / 365.25), 2)

df$title <- gsub('Cask: |1 ', '', df$title)

df$title <- trimws(sub("#.*", "", sub("/.*", "", df$title)))

# df$stored_at <- trimws(sub("(?i)distillery", "", sub(",.*", "", sub("-.*", "", sub(".*in bond by ", "", sub(".*storage at ", "", sub(".*(?i)Stored at ", "", sub(".*/", "", df$title))))))))

# df$stored_at_distillery <- df$stored_at == df$distillery

df$bottles_at_cask_strength <- round(df$bulk_litres / 0.7, 2)

df$buyer_price <- round(1.125 * as.numeric(df$price), 2)

df$buyer_price_per_bottle_at_cask_strength <- round(as.numeric(df$buyer_price) / df$bottles_at_cask_strength, 2)

df$buyer_price_per_litre_of_alcohol <- round(as.numeric(df$buyer_price) / df$rla, 2)

df$country <- NA
for (i in 1:nrow(df)){
  
  if (df$region[i] == 'Highlandq'){
    df$region[i] = 'Highlands'
  }
  
  if (df$region[i] %in% c('Highland', 'Lowland', 'Island')){
    df$region[i] = paste0(df$region[i], 's')
  }
  
  if (df$region[i] %in% c('Islay', 'Campbeltown', 'Highlands', 'Lowlands', 'Speyside', 'Islands')){
    df$country[i] = 'Scotland'
  } else if (grepl('USA', df$region[i])){
    df$country[i] = 'USA'
  } else {
    df$country[i] = df$region[i]
  }
  
}

df$auction_house <- 'WhiskyAuctioneer'

df$distillery <- str_to_title(df$distillery)

df[] <- lapply(df, function(x) replace(x, x == 'N/A', NA))

df <- df %>%
  select(-bottler, -cask_info, -size, -details, -age)

df <- rename(df, age = calculated_age,
                 url = link,
                 hammer_price = price,
                 auction_date = auction_end)

# colnames(df) <- gsub("_", " ", colnames(df))
# colnames(df) <- str_to_title(colnames(df))

df$hammer_price <- as.numeric(df$hammer_price)

df <- df %>%
  mutate(
    age = as.integer(age),
    rla = as.numeric(rla),
    bulk_litres = as.numeric(bulk_litres),
    bottles_at_cask_strength = as.numeric(bottles_at_cask_strength),
    buyer_price = as.numeric(buyer_price),
    buyer_price_per_bottle_at_cask_strength = as.numeric(buyer_price_per_bottle_at_cask_strength),
    buyer_price_per_litre_of_alcohol = as.numeric(buyer_price_per_litre_of_alcohol),
    auction_date = as.Date(auction_date, format='%d/%m/%Y'),
    filling_date = as.Date(filling_date, format='%d/%m/%Y'),
    regauged_date = as.Date(regauged_date, format='%d/%m/%Y')
  )

df <- df %>%
  select(-image, -year_ditilled)

setwd("C:\\Users\\joaov\\Documents\\Whisky Casks ETL\\silver")

write.csv(df, file = "whisky_auctioneer.csv", row.names = FALSE, fileEncoding = "UTF-8")
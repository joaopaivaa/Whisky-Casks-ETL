import pandas as pd
import re
import time
import numpy as np

def get_html(url):

    import requests
    from bs4 import BeautifulSoup

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return BeautifulSoup(response.text, 'html.parser')

def dms_to_degree(degrees, minutes, seconds, direction):

    decimal = float(degrees) + float(minutes) / 60 + float(seconds) / 3600
    decimal = round(decimal, 6)

    if direction in ('S', 'W'):
        decimal = -decimal
    
    return decimal

detailed_characteristics_mapping = {
    "Location": "location",
    "Coordinates": None,
    "Owner": 'owner',
    "Founded": 'founded',
    "Status": 'status',
    "Water source": "water_source",
    "No. of stills": "numb_stills",
    "Capacity": 'capacity',
    "Website": 'website',
    "Mothballed": 'mothballed',
    "Age(s)": 'age_s',
    "Cask type(s)": "cask_type_s",
    "Founder": "founder",
    "ABV": "abv",
    "Type": "type",
    "Architect": "architect",
    "Opened": "opened",
    "Annual production volume": "annual_production_volume",
    "Owned by": "owned_by",
    "Characteristics": "characteristics",
    "Company type": "company_type",
    "Industry": "industry"}

# df = pd.read_csv('C:\\Users\\joaov\\Documents\\Cask Tracker\\Full Database.csv')
# df = df[['Title', 'Rla', 'Distillery', 'Bulk Litres', 'Filling Date','Regauged Date', 'Calculated Age', 'Cask Type', 'Stored At', 'Strength']]

# df = df.dropna().reset_index(drop=True)

# distilleries = df['Distillery'].unique()
# distilleries = [distillery for distillery in distilleries if distillery != 'See Lot Description']

# distilleries_lower = [distillery.lower().replace(" ", "_") for distillery in distilleries]

# distilleries_info = pd.DataFrame(columns=list(detailed_characteristics_mapping) + ['Latitude', 'Longitude', 'Image'])
# distilleries_info['distillery'] = distilleries

distilleries_table_page = get_html('https://en.wikipedia.org/wiki/List_of_whisky_distilleries_in_Scotland')

lines = []

distilleries_table_1 = distilleries_table_page.select('table.wikitable')[0].select('tr')[1:]
for line in distilleries_table_1:
    line = line.select('td')
    distillery = line[0].text.strip()
    location = line[1].text.strip()
    region = line[2].text.strip()
    foundation_year = line[3].text.strip()
    owner = line[4].text.strip()
    closing_year = None
    is_closed = False
    is_demolished = False
    type = 'Malt Whisky'
    url = "https://en.wikipedia.org" + line[0].select('a')[0]['href']
    lines.append([distillery, location, region, foundation_year, owner, closing_year, is_closed, is_demolished, type, url])

distilleries_table_2 = distilleries_table_page.select('table.wikitable')[1].select('tr')[1:]
for line in distilleries_table_2:
    line = line.select('td')
    distillery = line[0].text.strip()
    if distillery == 'Loch Lomond':
        continue
    location = line[1].text.strip()
    region = None
    foundation_year = line[2].text.strip()
    owner = line[3].text.strip()
    closing_year = None
    is_closed = False
    is_demolished = False
    type = 'Grain Whisky'
    url = "https://en.wikipedia.org" + line[0].select('a')[0]['href']
    lines.append([distillery, location, region, foundation_year, owner, closing_year, is_closed, is_demolished, type, url])

distilleries_table_3 = distilleries_table_page.select('table.wikitable')[2].select('tr')[1:]
for line in distilleries_table_3:
    line = line.select('td')
    distillery = line[0].text.strip()
    location = line[1].text.strip()
    region = line[2].text.strip()
    foundation_year = None
    owner = None
    closing_year = line[3].text.split(',')[0].strip()
    is_closed = True
    is_demolished = 'demolished' in line[3].text.split(',')[1]
    type = 'Malt Whisky'
    url = "https://en.wikipedia.org" + line[0].select('a')[0]['href']
    lines.append([distillery, location, region, foundation_year, owner, closing_year, is_closed, is_demolished, type, url])

distilleries_table_4 = distilleries_table_page.select('table.wikitable')[3].select('tr')[1:]
for line in distilleries_table_4:
    line = line.select('td')
    distillery = line[0].text.strip()
    location = line[1].text.strip()
    region = None
    foundation_year = None
    owner = None
    closing_year = line[2].text.split(',')[0].strip()
    is_closed = True
    is_demolished = 'demolished' in line[2].text.split(',')[1]
    type = 'Grain Whisky'
    url = "https://en.wikipedia.org" + line[0].select('a')[0]['href']
    lines.append([distillery, location, region, foundation_year, owner, closing_year, is_closed, is_demolished, type, url])

distilleries_table = pd.DataFrame(lines, columns=['Distillery', 'Location', 'Region', 'Foundation Year', 'Owner', 'Closing Year', 'Is Closed', 'Is Demolished', 'Type of Whisky', 'URL'])

for column in list(detailed_characteristics_mapping) + ['Latitude', 'Longitude', 'Image']:
    distilleries_table[column] = None

for j in distilleries_table.index:

    distillery_url = distilleries_table['URL'].values[j]

    try:
        main_page = get_html(distillery_url)
    except Exception as e:
        continue
    
    if len(main_page.select('span.geo-default')) == 0:
        coordinates = None
        latitude = None
        longitude = None
    else:
        coordinates = main_page.select('span.geo-default')[0].text

        if '/' in coordinates:

            latitude = coordinates.split('/')[1].split(';')[0].strip()
            longitude = coordinates.split('/')[1].split(';')[1].strip()

        else:

            latitude = coordinates.split(' ')[0]
            degrees = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", latitude).group(1)
            minutes = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", latitude).group(2)
            seconds = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", latitude).group(2)
            direction = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", latitude).group(4)
            latitude = dms_to_degree(degrees, minutes, seconds, direction)

            longitude = coordinates.split(' ')[1]
            degrees = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", longitude).group(1)
            minutes = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", longitude).group(2)
            seconds = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", longitude).group(2)
            direction = re.search(r"(\d+)°(\d+)′([\d.]+)″([NSEW])", longitude).group(4)
            longitude = dms_to_degree(degrees, minutes, seconds, direction)
    
    distilleries_table.loc[j, 'Latitude'] = latitude
    distilleries_table.loc[j, 'Longitude']  = longitude
    
    if len(main_page.select('table.infobox')) == 0:
        continue

    if len(main_page.select('table.infobox')[0].select('a.mw-file-description')) > 0:
        image = main_page.select('table.infobox')[0].select('a.mw-file-description')[0]['href']
    else:
        image = None
    
    distilleries_table.loc[j, 'Image']  = image

    labels = main_page.select('table.infobox')[0].select('th.infobox-label')
    values = main_page.select('table.infobox')[0].select('td.infobox-data')

    for i in range(len(labels)):

        label = labels[i].text
        if label == 'Founded':
            
            for i in range(len(labels)):

                label = labels[i].text
                value = values[i].text

                # if label not in detailed_characteristics_mapping:
                #     raise ValueError(f"unknown info field: '{label}'")
                if label in detailed_characteristics_mapping:
                    if detailed_characteristics_mapping[label] is None:
                        continue
                    distilleries_table.loc[j, label] = value
            
            break
    
    time.sleep(1)

distilleries_table['Region'] = (
    np.where(distilleries_table['Region'] == 'Lowland', 'Lowlands',
    np.where(distilleries_table['Region'] == 'Highland', 'Highlands',
    np.where(distilleries_table['Region'] == 'Island', 'Highlands',
     distilleries_table['Region'])))
)

distilleries_table.loc[distilleries_table['Distillery'] == 'Cameronbridge', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Girvan', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Invergordon', 'Region'] = 'Highlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Loch Lomond', 'Region'] = 'Highlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'North British', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Starlaw', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Strathclyde', 'Region'] = 'Lowlands'

distilleries_table.loc[distilleries_table['Distillery'] == 'Caledonian', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Cambus', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Carsebridge', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Dumbarton', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Dundashill', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Garnheath', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Gartloch', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Kirkliston', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'North of Scotland', 'Region'] = 'Lowlands'
distilleries_table.loc[distilleries_table['Distillery'] == 'Port Dundas', 'Region'] = 'Lowlands'

distilleries_table.to_csv('dim_distilleries_info/dim_distilleries_info.csv', sep=';', index=False, encoding='utf-8')
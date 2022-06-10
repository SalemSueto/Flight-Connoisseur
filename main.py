import requests
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# --- USER Input --- #
flight_data_filename = "./flight_data.txt"
tequila_api = "Check The README file on how to obtain your personal Tequila API code"

# --- Preparation --- #
flight_data = pd.read_csv(flight_data_filename, sep="\t")    # Read the Input Data
tequila_location_endpoint = "https://tequila-api.kiwi.com/locations/query?"    # Tequila's Location Endpoint
tequila_search_endpoint = "https://tequila-api.kiwi.com/v2/search"

# --- Identify Airport IATA code for each departure/arrival city --- #
# In case there are multiple airports in the same city -> choose the one with the highest rank
iata_fixed = False
for idx, row in flight_data.iterrows():
    # From City
    if row.From_IATA == "":
        iata_fixed = True
        tequila_param = {"term": row.From_City, "location_types": "city"}
        tequila_headers = {"apikey": tequila_api}
        response = requests.get(url=tequila_location_endpoint, params=tequila_param, headers=tequila_headers)
        response.raise_for_status()
        rank_start = 10000000000
        for n in response.json()["locations"]:
            if n["country"]["name"] == row.From_Country and n["rank"] < rank_start:
                flight_data.loc[idx, 'From_IATA'] = n["code"]
                rank_start = n["rank"]
    # To City
    if row.To_IATA == "":
        iata_fixed = True
        tequila_param = {"term": row.To_City, "location_types": "city"}
        tequila_headers = {"apikey": tequila_api}
        response = requests.get(url=tequila_location_endpoint, params=tequila_param, headers=tequila_headers)
        response.raise_for_status()
        rank_start = 10000000000
        for n in response.json()["locations"]:
            if n["country"]["name"] == row.To_Country and n["rank"] < rank_start:
                flight_data.loc[idx, 'To_IATA'] = n["code"]
                rank_start = n["rank"]

# Save in case the IATA code were added
if iata_fixed:
    flight_data.to_csv("flight_data_iata_fixed.txt", sep='\t', mode='a', index=None)

# --- Search Flights --- #
# Find Flights filtered by the Price and Transfers
flight_search_list = []
today = datetime.now().strftime("%d/%m/%Y")
six_months_future = (date.today() + relativedelta(months=+6)).strftime("%d/%m/%Y")

for idx, row in flight_data.iterrows():
    # Tequila Search Request
    tequila_parameters = {"fly_from": row.From_IATA,
                          "fly_to": row.To_IATA,
                          "dateFrom": today,
                          "dateTo": six_months_future}
    tequila_headers = {"apikey": tequila_api}
    response = requests.get(url=tequila_search_endpoint, params=tequila_parameters, headers=tequila_headers)
    response.raise_for_status()
    flight_search_df = pd.json_normalize(response.json()["data"])

    if flight_search_df.empty:
        print(f"There is no flight between {row.From_City}-{row.To_City} in the next 6 months.")
    else:
        # Filter for the Max_Price
        flight_search_df = flight_search_df.loc[flight_search_df['price'] <= row.Max_Price]
        # Filter for the number of Transfers
        for idx2, row2 in flight_search_df.iterrows():
            flight_search_df.loc[idx2, 'transfers'] = len(flight_search_df.loc[idx2, 'route'])
        flight_search_df = flight_search_df.loc[flight_search_df['transfers'] <= row.Max_Transfer]
        if flight_search_df.empty:
            print(f"There is no flight between {row.From_City}-{row.To_City} in the next 6 months "
                  f"with the selected filters.")
        else:
            flight_search_list.append(flight_search_df)

# Combine the results from each search
flight_search_final = ""
if len(flight_search_list) > 0:
    flight_search_final = flight_search_list[0]
    if len(flight_search_list) > 1:
        for n in range(1, len(flight_search_list)):
            #flight_search_final = flight_search_final.concat([flight_search_final, flight_search_list[n]], axis=0)
            flight_search_final = pd.concat([flight_search_final, flight_search_list[n]], axis=0)

flight_search_final.to_csv("flight_search_all_info.txt", sep='\t', mode='a', index=False)

select_cols = ["id", "cityFrom", "countryFrom.name", "cityTo", "countryTo.name", "utc_departure", "utc_arrival",
               "bags_price.1", "bags_price.2", "availability.seats", "transfers", "price", "conversion.EUR"]
flight_search_final_cols_filter = flight_search_final[select_cols]
flight_search_final_cols_filter.to_csv("flight_search_filtered_cols.txt", sep='\t', mode='a', index=False)

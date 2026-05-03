import sqlalchemy
import json
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

# creating connection to Postgres
db_engine = sqlalchemy.create_engine("postgresql+psycopg2://dia:dia@localhost:5432/db")

def extract(file_path):
    # load JSON file
    with open(file_path, "r", encoding="utf-8") as f:
        station_data = json.load(f)

    records = []
    for station in station_data["result"]:
        # all evaNumbers where isMain == True
        main_eva = [e for e in station.get(
            "evaNumbers", []) if e.get("isMain")]

        # selection of all relevant data
        for eva in main_eva:
            coords = eva.get("geographicCoordinates", {}).get(
                "coordinates", [None, None])
            longitude, latitude = coords[0], coords[1]
            records.append({
                "station_number": station.get("number"),
                "ifopt": station.get("ifopt"),
                "station_name": station.get("name"),
                "eva_number": eva.get("number"),
                "longitude": longitude,
                "latitude": latitude  
            })

    return pd.DataFrame(records)

# extraction of all relevant data and save it in Dataframe "df"
df = extract('station_data.json')

# check if all data was extracted
print(df)
print(len(df), "lines extracted")

# write extracted df-data into Postgres database
df.to_sql("station", con=db_engine, index=False, if_exists="replace")

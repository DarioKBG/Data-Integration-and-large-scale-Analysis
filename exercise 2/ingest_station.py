import sqlalchemy
import json
import pandas as pd

# connect to PostgreSQL database
db_engine = sqlalchemy.create_engine(
    "postgresql+psycopg2://dia:dia@localhost:5433/postgres")

# Extract structured station records
def extract(file_path):
    # load json
    with open(file_path, "r", encoding="utf-8") as f:
        station_data = json.load(f)

    records = []
    for station in station_data["result"]:
        # all evaNumbers where isMain == True
        main_eva = [e for e in station.get(
            "evaNumbers", []) if e.get("isMain")]

        for eva in main_eva:
            coords = eva.get("geographicCoordinates", {}).get(
                "coordinates", [None, None])
            longitude, latitude = coords[0], coords[1]
            records.append({
                "station_number": station.get("number"),
                "ifopt": station.get("ifopt"),
                "station_name": station.get("name"),
                "eva_number": eva.get("number"),
                "latitude": latitude,
                "longitude": longitude
            })

    return pd.DataFrame(records)

# extract data
df = extract('../data/station_data.json')
print(df)
print(len(df), "lines extracted")

# load Dataframe into Postgres
df.to_sql("stations", con=db_engine, index=False, if_exists="replace")

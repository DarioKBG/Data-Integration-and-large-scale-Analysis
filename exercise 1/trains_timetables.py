import sqlalchemy
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
import os

# parse single XML-file
def parse_timetable_xml(path):
    try:
        tree = ET.parse(path)
    except Exception as e:
        print("Fehler in:", path, e)
        return []
    
    root = tree.getroot()

    station_name = root.attrib.get("station")
    rows = []

    # selection of all relevant data
    for s in root.findall("s"):
        tl = s.find("tl")
        ar = s.find("ar")
        dp = s.find("dp")

        rows.append({
            "station_name": station_name,
            "stop_id": s.attrib.get("id"),
            "train_type": tl.attrib.get("c") if tl is not None else None,
            "train_number": tl.attrib.get("n") if tl is not None else None,
            "arrival_time": ar.attrib.get("pt") if ar is not None else None,
            "departure_time": dp.attrib.get("pt") if dp is not None else None,
            "arrival_platform": ar.attrib.get("pp") if ar is not None else None,
            "departure_platform": dp.attrib.get("pp") if dp is not None else None,
            "arrival_path": ar.attrib.get("ppth") if ar is not None else None,
            "departure_path": dp.attrib.get("ppth") if dp is not None else None
        })
    return rows


def load_all_timetables(base_path="timetables_compressed"):
    all_rows = []

    for xml_file in Path(base_path).rglob("*timetable.xml"):
        all_rows.extend(parse_timetable_xml(xml_file))

    return pd.DataFrame(all_rows)

if __name__ == "__main__":
    # load and parse all XML-files
    df = load_all_timetables("timetables_compressed")
    #print(df.head())
    #print(len(df), "train stops extracted")
    print("-----------------")

    # create connection to Postgres
    db_engine = sqlalchemy.create_engine("postgresql+psycopg2://dia:dia@localhost:5432/db")

    # write data from Dataframe "df" into the database
    df.to_sql("trains_t", db_engine, index=False, if_exists="replace")

    print("success")

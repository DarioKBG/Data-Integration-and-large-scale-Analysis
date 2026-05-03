import sqlalchemy
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from datetime import datetime

# Derive a snapshot timestamp (rounded to the hour) from a stop ID string
def extract_date_hour_from_stop_id(stop_id):
    parts = stop_id.split("-")
    if len(parts) >= 3:
        time_str = parts[-2]
        dt = datetime.strptime(time_str[:10], "%y%m%d%H%M")
        return dt.replace(minute=0, second=0)

# Parse one planned timetable XML file into a list of row dictionaries
def parse_timetable_xml(path):
    tree = ET.parse(path)

    root = tree.getroot()
    station_name = root.attrib.get("station")
    rows = []

    for s in root.findall("s"):
        sid = s.attrib.get("id")
        snapshot_time = extract_date_hour_from_stop_id(sid)

        ar = s.find("ar")
        dp = s.find("dp")

        rows.append({
            "station_name": station_name,
            "stop_id": sid,
            "snapshot_time": snapshot_time,
            "arrival_time_planned": ar.attrib.get("pt") if ar is not None else None,
            "departure_time_planned": dp.attrib.get("pt") if dp is not None else None
        })
    return rows

# Load all planned timetable XML files under a directory tree into a single DataFrame
def load_all_timetables(base_path):
    rows = []
    for xml_file in Path(base_path).rglob("*timetable.xml"):
        rows.extend(parse_timetable_xml(xml_file))
    return pd.DataFrame(rows)

# Parse one “changes” XML file into a list of row dictionaries
def parse_timetable_change_xml(path):
    tree = ET.parse(path)

    root = tree.getroot()
    rows = []

    for s in root.findall("s"):
        sid = s.attrib.get("id")
        ar = s.find("ar")
        dp = s.find("dp")

        rows.append({
            "stop_id": sid,
            "arrival_time_actual": ar.attrib.get("ct") if ar is not None else None,
            "departure_time_actual": dp.attrib.get("ct") if dp is not None else None,
            "arrival_canceled": ar is not None and ar.attrib.get("cs") == "c",
            "departure_canceled": dp is not None and dp.attrib.get("cs") == "c"
        })
    return rows

# Load all change XML files under a directory tree into one DataFrame
def load_all_timetable_changes(base_path):
    rows = []
    for xml_file in Path(base_path).rglob("*change.xml"):
        rows.extend(parse_timetable_change_xml(xml_file))
    return pd.DataFrame(rows)


if __name__ == "__main__":

    print("--Load plan timetables--")
    df_plan = load_all_timetables("timetables_compressed")

    print("--Load change timetables--")
    df_actual = load_all_timetable_changes("timetable_changes_compressed")

    # test printing
    print("df_plan columns:", df_plan.columns.tolist())
    print("df_actual columns:", df_actual.columns.tolist())

    print("--Merge plan + actual--")
    df = pd.merge(df_plan, df_actual, on="stop_id", how="left")

    def compute_delay(pt, ct):

        if pd.notnull(pt) and pd.notnull(ct):
            dt_pt = datetime.strptime(str(pt), "%y%m%d%H%M")
            dt_ct = datetime.strptime(str(ct), "%y%m%d%H%M")
            return (dt_ct - dt_pt).total_seconds() / 60
        return None

    df["arrival_delay"] = df.apply(
        lambda r: compute_delay(r["arrival_time_planned"], r["arrival_time_actual"]), axis=1)
    df["departure_delay"] = df.apply(
        lambda r: compute_delay(r["departure_time_planned"], r["departure_time_actual"]), axis=1)

    db_engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://dia:dia@localhost:5433/postgres"
    )

    print("--Load station name mapping--")
    df_mapping = pd.read_sql(
        "SELECT xml_station_name, matched_station FROM station_name_mapping",
        db_engine
    )

    df = df.merge(
        df_mapping,
        left_on="station_name",
        right_on="xml_station_name",
        how="left"
    )

    df["station_name"] = df["matched_station"]

    df.drop(columns=["xml_station_name", "matched_station"], inplace=True)

    df.to_sql("trains", db_engine, index=False,
              if_exists="replace", method="multi")

    print("--finished--")

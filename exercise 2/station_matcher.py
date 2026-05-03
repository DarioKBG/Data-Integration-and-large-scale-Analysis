import json
import pandas as pd
from sqlalchemy import create_engine
from rapidfuzz import process, fuzz

# load station_data.json
with open("station_data.json", encoding="utf-8") as f:
    station_data = json.load(f)

json_names = [s["name"] for s in station_data["result"]]

# connect to PostgreSQL database
db_engine = create_engine(
    "postgresql+psycopg2://dia:dia@localhost:5433/postgres"
)

# Pull distinct station names from the trains table
query = """
SELECT DISTINCT station_name
FROM trains
WHERE station_name IS NOT NULL;
"""
df_xml = pd.read_sql(query, db_engine)

# Load manual override mappings from CSV
df_overrides = pd.read_csv("station_manual_overrides.csv")
manual_map = dict(
    zip(df_overrides["xml_station_name"], df_overrides["matched_station"])
)

# For each XML station name, produce a best match
matches = []
for name in df_xml["station_name"]:

    # 1️⃣ Manual override has highest priority
    if name in manual_map:
        matches.append({
            "xml_station_name": name,
            "matched_station": manual_map[name],
            "similarity_score": 100,
            "match_type": "manual"
        })
        continue

    # use rapidfuzz
    match, score, _ = process.extractOne(
        name,
        json_names,
        scorer=fuzz.token_set_ratio
    )

    matches.append({
        "xml_station_name": name,
        "matched_station": match,
        "similarity_score": score,
        "match_type": "auto"
    })

# Build a DataFrame of all matches
df_matches = pd.DataFrame(matches)

# adjust threshold
THRESHOLD = 70

# Split “good matches” vs “unmatched” by a threshold
df_good = df_matches[df_matches["similarity_score"] >= THRESHOLD]
df_unmatched = df_matches[df_matches["similarity_score"] < THRESHOLD]

# Write accepted matches to Postgres
df_good.to_sql(
    "station_name_mapped",
    db_engine,
    index=False,
    if_exists="replace"
)

# df_unmatched.to_csv(
#    "station_unmatched.csv",
#    index=False
# )

print("Mapping complete")

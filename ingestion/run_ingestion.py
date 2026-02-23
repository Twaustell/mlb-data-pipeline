import os
import json
from datetime import datetime
from ingestion.mlb_api import get_schedule_by_date, get_game_boxscore


RAW_DATA_DIR = "data/raw"


def save_json(data: dict, date: str, game_pk: int):
    year, month, day = date.split("-")

    partition_path = os.path.join(
        RAW_DATA_DIR,
        f"year={year}",
        f"month={month}",
        f"day={day}"
    )

    os.makedirs(partition_path, exist_ok=True)

    filepath = os.path.join(partition_path, f"game_{game_pk}.json")

    if os.path.exists(filepath):
        print(f"File already exists, skipping {filepath}")
        return

    with open(filepath, "w") as f:
        json.dump(data, f)

    print(f"Saved {filepath}")



def ingest_date(date: str):
    print(f"Ingesting games for {date}")

    schedule = get_schedule_by_date(date)

    dates = schedule.get("dates", [])
    if not dates:
        print("No games found.")
        return

    games = dates[0].get("games", [])

    for game in games:
        game_pk = game["gamePk"]
        boxscore = get_game_boxscore(game_pk)

        #filename = f"{date}_game_{game_pk}.json"
        save_json(boxscore, date, game_pk)



if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python ingestion/run_ingestion.py YYYY-MM-DD")
        sys.exit(1)

    ingest_date(sys.argv[1])
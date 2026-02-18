import os
import json
import pandas as pd

RAW_DATA_DIR = "data/raw"
SILVER_DATA_DIR = "data/silver"


def parse_boxscore(filepath: str, game_pk: int, date: str):
    with open(filepath, "r") as f:
        data = json.load(f)

    teams = data["teams"]
    rows = []

    for side in ["home", "away"]:
        team_data = teams[side]

        rows.append({
            "game_pk": game_pk,
            "team_id": team_data["team"]["id"],
            "team_name": team_data["team"]["name"],
            "home_away": side,
            "runs": team_data["teamStats"]["batting"].get("runs", 0),
            "hits": team_data["teamStats"]["batting"].get("hits", 0),
            "errors": team_data["teamStats"]["fielding"].get("errors", 0),
            "date": date
        })

    return rows


def transform_date(year: str, month: str, day: str):
    partition_path = os.path.join(
        RAW_DATA_DIR,
        f"year={year}",
        f"month={month}",
        f"day={day}"
    )

    if not os.path.exists(partition_path):
        print("Raw partition not found.")
        return

    all_rows = []

    for file in os.listdir(partition_path):
        if file.endswith(".json"):
            filepath = os.path.join(partition_path, file)
            game_pk = int(file.replace("game_", "").replace(".json", ""))
            date_str = f"{year}-{month}-{day}"
            #rows = parse_boxscore(filepath)
            rows = parse_boxscore(filepath, game_pk, date_str)
            all_rows.extend(rows)

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("No data found.")
        return

    silver_partition = os.path.join(
        SILVER_DATA_DIR,
        f"year={year}",
        f"month={month}",
        f"day={day}"
    )

    os.makedirs(silver_partition, exist_ok=True)

    output_path = os.path.join(silver_partition, "team_game_stats.parquet")

    df.to_parquet(output_path, index=False)

    print(f"Saved {output_path}")


if __name__ == "__main__":
    #HARD CODED NOW DURING THE OFFSEASON
    transform_date("2025", "09", "28")

import os
import json
import pandas as pd
import sys
import boto3
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

RAW_DATA_DIR = "data/raw"
SILVER_DATA_DIR = "data/silver"


def parse_boxscore(filepath: str, game_pk: int, date: str):
    with open(filepath, "r") as f:
        data = json.load(f)

    teams = data["teams"]

    home_data = teams["home"]
    away_data = teams["away"]

    home_runs = home_data["teamStats"]["batting"].get("runs", 0)
    away_runs = away_data["teamStats"]["batting"].get("runs", 0)

    rows = []

    # Home row
    rows.append({
        "game_pk": game_pk,
        "team_id": home_data["team"]["id"],
        "team_name": home_data["team"]["name"],
        "home_away": "home",
        "runs": home_runs,
        "hits": home_data["teamStats"]["batting"].get("hits", 0),
        "errors": home_data["teamStats"]["fielding"].get("errors", 0),
        "run_differential": home_runs - away_runs,
        "win_flag": 1 if home_runs > away_runs else 0,
        "date": date
    })

    # Away row
    rows.append({
        "game_pk": game_pk,
        "team_id": away_data["team"]["id"],
        "team_name": away_data["team"]["name"],
        "home_away": "away",
        "runs": away_runs,
        "hits": away_data["teamStats"]["batting"].get("hits", 0),
        "errors": away_data["teamStats"]["fielding"].get("errors", 0),
        "run_differential": away_runs - home_runs,
        "win_flag": 1 if away_runs > home_runs else 0,
        "date": date
    })

    return rows


#def upload_to_s3(local_path: str, bucket: str, s3_key: str):
#    s3 = boto3.client("s3")
#
#    logger.info(f"Uploading {local_path} to s3://{bucket}/{s3_key}")
#    s3.upload_file(local_path, bucket, s3_key)
#    logger.info("Upload complete.")

def upload_to_s3(local_path, s3_key):
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")

    logger.info(f"Uploading {local_path} to s3://{S3_BUCKET}/{s3_key}")

    s3 = boto3.client("s3")
    s3.upload_file(local_path, S3_BUCKET, s3_key)

    logger.info("Upload complete.")


def transform_date(date_str: str):
    year, month, day = date_str.split("-")
    partition_path = os.path.join(
        RAW_DATA_DIR,
        f"year={year}",
        f"month={month}",
        f"day={day}"
    )

    if not os.path.exists(partition_path):
        logger.info("Raw partition not found.")
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
        logger.info("No data found.")
        return

    silver_partition = os.path.join(
        SILVER_DATA_DIR,
        f"year={year}",
        f"month={month}",
        f"day={day}"
    )


    os.makedirs(silver_partition, exist_ok=True)

    output_path = os.path.join(silver_partition, "team_game_stats.parquet")

    # Idempotent check
    if os.path.exists(output_path):
        logger.info(f"Silver file already exists for {year}-{month}-{day}. Skipping.")
        return

    df.to_parquet(output_path, index=False)

    logger.info(f"Saved {output_path}")   

    #bucket_name = "mlb-data-pipeline-956959164726"

    s3_key = f"silver/year={year}/month={month}/day={day}/team_game_stats.parquet"

    upload_to_s3(output_path, s3_key)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.info("Usage: python ingestion/transform.py YYYY-MM-DD")
        sys.exit(1)

    input_date = sys.argv[1]

    transform_date(input_date)

    


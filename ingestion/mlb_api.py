import requests

BASE_URL = "https://statsapi.mlb.com/api/v1"


def get_schedule_by_date(date: str):
    """
    Fetch MLB schedule for a specific date.
    Date format: YYYY-MM-DD
    """
    url = f"{BASE_URL}/schedule?sportId=1&date={date}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_game_boxscore(game_pk: int):
    """
    Fetch boxscore data for a specific game.
    """
    url = f"{BASE_URL}/game/{game_pk}/boxscore"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

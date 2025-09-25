from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import create_view
import requests

default_args = {
    "owner": "data_eng",
    "start_date": datetime(2025, 9, 25),
    "retries": 1
}

engine = create_engine("postgresql://user:password@finalDataeng:5432/football_db_new")

def fetch_and_save_bronze():
    API_URL = "https://statsbanger.onrender.com/api/player-season-stat"
    PARAMS = {"uniqueTournamentID": 8, "seasonID": 52376}
    resp = requests.get(API_URL, params=PARAMS)
    resp.raise_for_status()
    data = resp.json()
    df = pd.json_normalize(data)
    df.to_sql("player_stats_bronze", engine, if_exists="replace", index=False)

def transform_bronze_to_silver():
    bronze_df = pd.read_sql("SELECT * FROM player_stats_bronze", engine)
    silver_df = pd.DataFrame({
        "player_id": bronze_df["player.playerId"],
        "player_name": bronze_df["player.name"],
        "birthday": pd.to_datetime(bronze_df["player.birthdayTimestamp"]),
        "age": bronze_df["player.age"],
        "position": bronze_df["player.position"], 
        "nationality": bronze_df["player.nationality"],
        "team_id": bronze_df["team.teamId"],
        "team_name": bronze_df["team.name"],
        "season_id": bronze_df["seasonId"],
        "unique_tournament_id": bronze_df["uniqueTournamentId"],
        "appearances": bronze_df["stats.appearances"],
        "goals": bronze_df["stats.goals"],
        "assists": bronze_df["stats.assists"],
        "minutes_played": bronze_df["stats.minutes_played"],
        "yellow_cards": bronze_df["stats.yellow_cards"],
        "red_cards": bronze_df["stats.red_cards"],
        "shots_on_target": bronze_df["stats.shots_on_target"],
        "shots_off_target": bronze_df["stats.shots_off_target"],
        "passes_total": bronze_df["stats.total_passes"],
        "passes_accurate": bronze_df["stats.accurate_passes"],
        "passes_accuracy_percent": bronze_df["stats.accurate_passes_percentage"],
        "tackles": bronze_df["stats.tackles"],
        "interceptions": bronze_df["stats.interceptions"],
        "fouls": bronze_df["stats.fouls"],
        "possession_lost": bronze_df["stats.possession_lost"]
    })
    silver_df.to_sql("player_stats", engine, if_exists="replace", index=False)

def clean_silver_data():
    silver_df = pd.read_sql("""SELECT player_id, player_name, team_id, team_name, season_id, goals, 
                            assists, minutes_played, appearances, yellow_cards, red_cards, tackles, interceptions, 
                            fouls, passes_accuracy_percent FROM player_stats""", engine)
    
    #missing text fields
    silver_df["player_name"].fillna("Unknown", inplace=True)
    silver_df["team_name"].fillna("Unknown", inplace=True)
    
    #missing numeric fields
    numeric_cols = ["goals","assists","minutes_played","appearances","yellow_cards","red_cards","tackles","interceptions","fouls","passes_accuracy_percent"]
    silver_df[numeric_cols] = silver_df[numeric_cols].fillna(0)
    
    #remove duplicates
    silver_df.drop_duplicates(subset=["player_id","season_id"], inplace=True)
    
    #ensure types
    silver_df = silver_df.astype({
        "player_id": int,
        "team_id": int,
        "season_id": int
    })
    
    silver_df.to_sql("player_stats_clean", engine, if_exists="replace", index=False)


def transform_silver_to_gold():
    silver_df = pd.read_sql("""
        SELECT 
            player_id, player_name, team_id, team_name, season_id,
            goals, assists, appearances, minutes_played, 
            yellow_cards, red_cards, tackles, interceptions, fouls,
            passes_accuracy_percent
        FROM player_stats
    """, engine)
    
    df = silver_df.copy()
    
    df["goal_per_appearance"] = df["goals"] / df["appearances"].replace(0, 1)
    df["assist_per_appearance"] = df["assists"] / df["appearances"].replace(0, 1)
    df["goal_assist_per_90"] = (df["goals"] + df["assists"]) / df["minutes_played"].replace(0,1) * 90
    df["yellow_per_appearance"] = df["yellow_cards"] / df["appearances"].replace(0,1)
    df["red_per_appearance"] = df["red_cards"] / df["appearances"].replace(0,1)
    df["tackles_per_90"] = df["tackles"] / df["minutes_played"].replace(0,1) * 90
    df["interceptions_per_90"] = df["interceptions"] / df["minutes_played"].replace(0,1) * 90
    df["fouls_per_90"] = df["fouls"] / df["minutes_played"].replace(0,1) * 90

    gold_columns = [
        "player_id", "player_name", "team_id", "team_name", "season_id",
        "goals", "assists", "appearances", "minutes_played",
        "goal_per_appearance", "assist_per_appearance", "goal_assist_per_90",
        "passes_accuracy_percent", "yellow_cards", "yellow_per_appearance",
        "red_cards", "red_per_appearance",
        "tackles", "tackles_per_90",
        "interceptions", "interceptions_per_90",
        "fouls", "fouls_per_90"
    ]

    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE player_stats_gold")
    
    df[gold_columns].to_sql("player_stats_gold", engine, if_exists="append", index=False)



with DAG("football_complete_etl", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    t1 = PythonOperator(
        task_id="fetch_bronze",
        python_callable=fetch_and_save_bronze
    )

    t2 = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_bronze_to_silver
    )

    t3 = PythonOperator(
        task_id="clean_silver_data",
        python_callable=clean_silver_data
    )

    t4 = PythonOperator(
        task_id="transform_to_gold",
        python_callable=transform_silver_to_gold
    )

    t5 = PythonOperator(
        task_id="create_views",
        python_callable=create_view.create_views,
        op_kwargs={"engine": engine}  
    )


    t1 >> t2 >> t3 >> t4 >> t5

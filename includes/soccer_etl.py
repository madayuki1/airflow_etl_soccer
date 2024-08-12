import pandas as pd
from sqlalchemy import create_engine
import sqlite3

def extract_sqlite_to_postgres(filepath):
    sqlite_conn = sqlite3.connect(filepath)
    cursor = sqlite_conn.cursor()

    cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name!="sqlite_sequence"')
    tables = cursor.fetchall()

    engine = create_engine('postgres+psycopg2://postgres:postgres@postgres-data/postgres')

    for table_name in tables:
        table_name = table_name[0]

        df = pd.read_sql(f'SELECT * FROM {table_name}', sqlite_conn)
        
        df.to_sql(name = f"bronze_{table_name.lower()}" , con = engine, if_exists = 'replace', index = False)

    sqlite_conn.close()

def preprocess_data():
    engine = create_engine('postgres+psycopg2://postgres:postgres@postgres-data/postgres')
    conn = engine.raw_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    )
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df.drop_duplicates(inplace=True)
        df.fillna(method="ffill", inplace=True)
        df.to_sql(name=f"{table_name.replace('bronze', 'silver')}", con = engine, if_exists='replace', index=False)
    cursor.close()
    conn.close()

def transform_and_load_data():
    engine = create_engine('postgres+psycopg2://postgres:postgres@postgres-data/postgres')
    conn = engine.raw_connection()

    df = pd.read_sql('''SELECT id, country_id, league_id, season, stage, date, home_team_api_id, away_team_api_id, 
                        home_team_goal, away_team_goal FROM silver_match''', conn)

    df_team = pd.read_sql(f'SELECT team_api_id, team_long_name FROM silver_team', conn)

    df_country = pd.read_sql(f'SELECT id, name FROM silver_country', conn)

    df_league = pd.read_sql(f'SELECT id, name FROM silver_league', conn)

    df_joined_home = pd.merge(df, df_team, left_on='home_team_api_id', right_on='team_api_id', how='inner')
    df_joined_home.rename(columns={'team_long_name': 'home_team'}, inplace=True)
    df_joined_home.drop(columns={'home_team_api_id', 'team_api_id'}, inplace=True)

    df_joined_away = pd.merge(df_joined_home, df_team, left_on='away_team_api_id', right_on='team_api_id', how='inner')
    df_joined_away.rename(columns={'team_long_name': 'away_team'}, inplace=True)
    df_joined_away.drop(columns={'away_team_api_id', 'team_api_id'}, inplace=True)

    df_joined_country= pd.merge(df_joined_away, df_country, left_on='country_id', right_on='id', how='inner', suffixes=('', '_remove')).drop({'id_remove', 'country_id'}, axis=1)
    df_joined_country.rename(columns={'name': 'country'}, inplace=True)

    df_joined_league= pd.merge(df_joined_country, df_league, left_on='league_id', right_on='id', how='inner', suffixes=('', '_remove')).drop({'id_remove', 'league_id'}, axis=1)
    df_joined_league.rename(columns={'name': 'league'}, inplace=True)

    column_names = ['id', 'country', 'league', 'season', 'stage', 'date', 'home_team', 'away_team', 'home_team_goal', 'away_team_goal']

    df_joined_league = df_joined_league.reindex(columns=column_names)
    df_joined_league.to_sql(name=f'gold_match', con=engine, if_exists='replace', index=False)
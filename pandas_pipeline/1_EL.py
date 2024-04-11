from sqlalchemy import create_engine
import pandas as pd
from conn_db import get_postgres_conn, get_mysql_conn


def extract():
    try:
        source_conn = get_postgres_conn()  
        engine = create_engine(source_conn)
        table = 'dbo.nba_forecast'
        query = f'SELECT * FROM {table}'
        df = pd.read_sql(query, engine)
        load(df, table)
    except Exception as e:
        print(f"Error during extraction: {str(e)}")


def load(df, table):
    try:
        target_conn = get_postgres_conn()
        with create_engine(target_conn).connect():
            df.to_sql(f"stg_{table}", target_conn, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error during loading: {str(e)}")


if __name__ == "__main__":
    extract(); load()
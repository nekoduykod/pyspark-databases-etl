import os
from dotenv import load_dotenv

load_dotenv()

def get_db_conn(postgresql=False):
    if postgresql:
        pg_uid = os.getenv("PG_UID")
        pg_pass = os.getenv("PG_PASS")
        return f"jdbc:postgresql://localhost:5432/MyDB?user={pg_uid}&password={pg_pass}"
    else:
        mysql_host = os.getenv("MYSQL_HOST")
        mysql_port = os.getenv("MYSQL_PORT")
        mysql_user = os.getenv("MYSQL_USER")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        mysql_database = os.getenv("MYSQL_DATABASE")
        return f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}\
            ?user={mysql_user}&password={mysql_password}"
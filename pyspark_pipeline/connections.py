import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession   
from pyspark.sql.functions import *  
 

load_dotenv()


def get_postgres_conn():
    pg_uid = os.getenv("PG_UID")
    pg_pass = os.getenv("PG_PASS")
    pg_db = os.getenv("PG_DB")
    return f"jdbc:postgresql://localhost:5432/{pg_db}?user={pg_uid}&password={pg_pass}"


def get_mysql_conn():
    mysql_host = os.getenv("MYSQL_HOST")
    mysql_port = os.getenv("MYSQL_PORT")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_database = os.getenv("MYSQL_DATABASE")
    return f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}?\
             user={mysql_user}&password={mysql_password}"


def create_spark_session():
    return SparkSession.builder \
        .appName("Big_Poppa") \
        .config("spark.jars", "D:\\projects\\pyspark_replication\\connector\\mysql-connector-j-8.1.0.jar,"
               "D:\\projects\\pyspark_replication\\connector\\postgresql-42.6.0.jar") \
        .getOrCreate()
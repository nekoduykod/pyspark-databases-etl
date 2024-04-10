import os
from dotenv import load_dotenv

from pyspark.sql.functions import *

from connections import get_mysql_conn, get_postgres_conn


load_dotenv()


def extract_from_mysql(spark, table):
    mysql_db_conn = get_mysql_conn()
    df = spark.read.format("jdbc").options(
        url=mysql_db_conn,
        mysql_driver="com.mysql.cj.jdbc.Driver",
        dbtable=table,
    ).load()
    return df

def extract_from_postgres(spark, table):
    postgres_db_conn = get_postgres_conn()
    df = spark.read.format("jdbc").options(
        url=postgres_db_conn,
        postgres_driver="com.postgresql.jdbc.Driver",
        dbtable=table,
    ).load()
    return df


def any_transform(df):
    df.select("Title", "`IMDb-Rating`", "ReleaseYear") \
      .filter(col("`IMDb-Rating`") > 7.5) \
      .filter(col("ReleaseYear") >= 2010) \
      .orderBy(asc("ReleaseYear"))
    return df


def load_to_postgres(spark, df, table):
    pg_db_conn = get_postgres_conn()
    df.write.jdbc(url=pg_db_conn, table=table, mode="append", 
                    properties={"user": os.getenv("PG_UID"), 
                            "password": os.getenv("PG_PASS"),\
                              "driver": "org.postgresql.Driver"}) 

def load_to_mysql(spark, df, table):
    mysql_db_conn = get_mysql_conn()
    df.write.jdbc(url=mysql_db_conn, table=table, mode="append", 
                  properties={"user": os.getenv("MYSQL_UID"), 
                          "password": os.getenv("MYSQL_PASS"), 
                            "driver": "com.mysql.jdbc.Driver"})
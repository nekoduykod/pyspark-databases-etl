from connections import get_db_conn
from pyspark.sql.functions import *  
from dotenv import load_dotenv
import os
 
load_dotenv()

def extract_from_mysql(spark, table):
    mysql_db_conn = get_db_conn(postgresql=False)
    df = spark.read.format("jdbc").options(
        url=mysql_db_conn,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=table,
    ).load()
    print("Data extracted")
    return df

def transform_df(df):
    df.select("Title", "`IMDb-Rating`", "ReleaseYear") \
    .filter(col("`IMDb-Rating`") > 7.5) \
    .filter(col("ReleaseYear") >= 2010) \
    .orderBy(asc("ReleaseYear"))
    print("Data transformed")
    return df

def load_to_postgres(spark, df, table):
    pg_db_conn = get_db_conn(postgresql=True)
    df.write.jdbc(url=pg_db_conn, table=f"stg_{table}", mode="overwrite", 
                    properties={"user": os.getenv("PG_UID"), "password": os.getenv("PG_PASS"),\
                                 "driver": "org.postgresql.Driver"}) 
    print("Data imported")
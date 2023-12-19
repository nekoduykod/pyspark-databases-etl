import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, SQLContext    
from pyspark.sql.functions import *  
from db_conn import get_db_conn 
 
load_dotenv()

def create_spark_session():
    return SparkSession.builder \
        .appName("Big_Poppa2") \
        .config("spark.jars", "D:\\projects\\pyspark_replication\\connector\\mysql-connector-j-8.1.0.jar,"
               "D:\\projects\\pyspark_replication\\connector\\postgresql-42.6.0.jar") \
        .getOrCreate()
     
def extract(spark, source_db_conn, table):
    df = spark.read.format("jdbc").options(
        url=source_db_conn,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=table,
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD")
    ).load()

    df_filtered = df.select("Title", "`IMDb-Rating`", "ReleaseYear") \
        .filter(col("`IMDb-Rating`") > 7.5) \
        .filter(col("ReleaseYear") >= 2010) \
        .orderBy(asc("ReleaseYear"))

    load(df_filtered, table, spark)
    print("Data extracted")

def load(df, table, spark):
    target_db_conn = get_db_conn(postgresql=True)
    df.write.jdbc(url=target_db_conn, table=f"stg_{table}", mode="overwrite", 
                    properties={"user": os.getenv("PG_UID"), "password": os.getenv("PG_PASS"), "driver": "org.postgresql.Driver"}) 
    print("Data imported successfully")

if __name__ == "__main__":
    spark = create_spark_session()
    source_db_conn = get_db_conn(postgresql=False)
    table_name = 'imdb_data_final'
    extract(spark, source_db_conn, table_name)
    spark.stop()
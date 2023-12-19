from pyspark.sql import SparkSession, SQLContext    
from pyspark.sql.functions import *  
from db_conn import get_db_conn 
import os
from dotenv import load_dotenv

def create_spark_session():
    return SparkSession.builder \
        .appName("Big_Poppa") \
        .config("spark.jars", "D:\\projects\\pyspark_replication\\connector\\mysql-connector-j-8.1.0.jar,"
               "D:\\projects\\pyspark_replication\\connector\\postgresql-42.6.0.jar") \
        .getOrCreate()
     
def extract(spark):
    source_db_conn = get_db_conn(postgresql=False)
    table = 'imdb_data_final'
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")

    df = spark.read.format("jdbc").options(
        url=source_db_conn,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=table,
        user=user,
        password=password
    ).load()

    load(df, table, spark)
    print("Data extracted")

def load(df, table, spark): 
    target_db_conn = get_db_conn(postgresql=True)
    pg_uid = os.getenv("PG_UID")
    pg_pass = os.getenv("PG_PASS")

    df.write.jdbc(url=target_db_conn, table=f"stg_{table}", mode="overwrite", 
                    properties={"user": pg_uid, "password": pg_pass,\
                                 "driver": "org.postgresql.Driver"}) 
    print("Data imported successfully") 

if __name__ == "__main__":        
    spark = create_spark_session()
    extract(spark)
    spark.stop()
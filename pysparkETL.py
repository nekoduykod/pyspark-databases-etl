import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, SQLContext    
from pyspark.sql.functions import *  
 
load_dotenv()

mysql_host = os.getenv("MYSQL_HOST")
mysql_port = os.getenv("MYSQL_PORT")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE")

pg_uid = os.getenv("PG_UID")
pg_pass = os.getenv("PG_PASS")

spark = SparkSession.builder \
    .appName("LETSGO") \
    .config("spark.jars", "D:\\projects\\pyspark_replication\\connector\\mysql-connector-j-8.1.0.jar,D:\\projects\\pyspark_replication\\connector\\postgresql-42.6.0.jar") \
    .getOrCreate() 
     
def extract():
    try:
        connection_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}?user={mysql_user}&password={mysql_password}"
        table = 'imdb_data_final'
        df = spark.read.format("jdbc").options(
            url=connection_url,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=table,
            user=mysql_user,
            password=mysql_password
        ).load()

        df_filtered = df.select("Title", "`IMDb-Rating`", "ReleaseYear") \
        .filter(col("`IMDb-Rating`") > 7.5) \
        .filter(col("ReleaseYear") >= 2010) \
        .orderBy(asc("ReleaseYear"))

        """ df_filtered = df.filter(col("`IMDb-Rating`") > 7.5) \    <== This snippet can include all columns of a source table
        .filter(col("ReleaseYear") >= 2010) \
        .orderBy(asc("ReleaseYear"))   """
     
        load(df_filtered, table)
    except Exception as e:
        print("Error occurred:", str(e))

def load(df, table): 
    try: 
        connection_string = f"jdbc:postgresql://localhost:5432/MyDB"
        df.write.jdbc(url=connection_string, table=f"tfmd_{table}", mode="overwrite", 
                      properties={"user": pg_uid, "password": pg_pass, "driver": "org.postgresql.Driver"}) 
        print("Data imported successfully") 
    except Exception as e: 
        print("Data load error:", str(e))

extract() 
spark.stop()

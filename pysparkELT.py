import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, SQLContext    
from pyspark.sql.functions import *  

# Load environment variables from .env file
load_dotenv()

# MySQL connection parameters
mysql_host = os.getenv("MYSQL_HOST")
mysql_port = os.getenv("MYSQL_PORT")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE")

# PostgreSQL connection parameters
pg_uid = os.getenv("PG_UID")
pg_pass = os.getenv("PG_PASS")

# Create a SparkSession with the MySQL & PostgreSQL connector JARs
spark = SparkSession.builder \
    .appName("COOL") \
    .config("spark.jars", "D:\\projects\\pyspark_replication\\connector\\mysql-connector-j-8.1.0.jar,D:\\projects\\pyspark_replication\\connector\\postgresql-42.6.0.jar") \
    .getOrCreate()  
     
# Extract data from MySQL using PySpark 
def extract():
    try:
        # MySQL connection URL
        connection_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}?user={mysql_user}&password={mysql_password}"
        # Read data into a Spark DataFrame
        table = 'imdb_data_final'
        df = spark.read.format("jdbc").options(
            url=connection_url,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=table,
            user=mysql_user,
            password=mysql_password
        ).load()
        # Process the data or perform any necessary operations
        load(df, table)
    except Exception as e:
        print("Error occurred:", str(e))

# Load data to PostgreSQL using PySpark 
def load(df, table): 
    try: 
        # PostgreSQL connection properties 
        connection_string = f"jdbc:postgresql://localhost:5432/MyDB"
        # Write data from DataFrame to PostgreSQL table 
        df.write.jdbc(url=connection_string, table=f"stg_{table}", mode="overwrite", 
                      properties={"user": pg_uid, "password": pg_pass, "driver": "org.postgresql.Driver"}) 
        print("Data imported successfully") 
    except Exception as e: 
        print("Data load error:", str(e))

# Call the extract function 
extract() 
# Stop the Spark session 
spark.stop()
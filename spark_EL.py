from etl_ops import extract_from_mysql, load_to_postgres
from connections import create_spark_session 

spark = create_spark_session()
table="imdb_data_final"

df = extract_from_mysql(spark, table=table)
load_to_postgres(spark, df, table=table)

spark.stop()
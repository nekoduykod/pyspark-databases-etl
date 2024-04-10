from etl_ops import extract_from_mysql, extract_from_postgres, any_transform, load_to_postgres, load_to_mysql
from connections import create_spark_session


spark = create_spark_session()

if extract_database = "postgresql":
    df = extract_from_mysql(spark, table="imdb_data_final")
else:
    df = extract_from_postgres(spark, table="imdb_data_final")
    
apply_transform = True
if apply_transform:
    df = any_transform(df)

if load_database = 'postgresql':
    load_to_postgres(spark, df, table='stg_imdb_data')
else:
    load_to_mysql(spark, df, table='stg_imdb_data')

spark.stop()
from pyspark_pipeline.etl_ops import extract_from_mysql, extract_from_postgres, any_transform, load_to_postgres, load_to_mysql
from pyspark_pipeline.connections import create_spark_session


databases = {
    "postgresql": {
        "extract": extract_from_postgres,
        "load": load_to_postgres
    },
    "mysql": {
        "extract": extract_from_mysql,
        "load": load_to_mysql
    }
}

# тут змінюй базу даних
extract_database = "postgresql"
load_database = "postgresql"  


if extract_database not in databases or load_database not in databases:
    raise ValueError("Specified database types are not supported")


spark = create_spark_session()

extract_function = databases[extract_database]["extract"]
df = extract_function(spark, table="imbd_dataset")


apply_transform = True
if apply_transform:
    df = any_transform(df)


load_function = databases[load_database]["load"]
load_function(spark, df, table="final_imdb_data")

spark.stop()
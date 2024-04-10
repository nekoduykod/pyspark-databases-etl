# I. PySpark ETL/EL

<img src="images/pyspark%20proj.jpg" />

## MySQL=>PostgreSQL PySpark pipeline

The scripts provide flexibility: ETL - MySQL data extraction => aggregations => loads results to PostgreSQL; EL - allowing transformations at the destination database (aka ELT)

## Prerequisites

- Python 3.10, PySpark 3.4.1 were used
- MySQL & PostgreSQL installed, a dataset from Kaggle
- Download MySQL & PostgreSQL connector JAR files. 
Windows users: Choose Java 8 and download winutils.exe from [here](https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin)
Linux/macOS users: ensure compatibility with correct versions of Hadoop and Spark.
- Add .env (JAVA_HOME, HADOOP_HOME)
- Disable Firewall, antivirus software, or create appropriate ingress rules

## Results

1. MySQL Dataset:   
<img src="images/MySQL%20imdb%20dataset.jpg" />

2. Data Loaded:
<img src="images/data%20imported.jpg" />

3. MySQL=>PostgreSQL using PySpark:
<img src="images/MySQL%20table%20PySpark%20loaded%20to%20Postgre.jpg" />

4. Data with transformation imported:
<img src="images/Pyspark%20trsfmd%20MySQL%20to%20Postgre.jpg" />
<img src="images/MySQL%20table%20PySpark%20tfmd_%20to%20Postgre.jpg" />


# II. Pandas ETL/EL

<img src="images/pandas_overview.png" alt="blueprint" width="83%" />

A small simple task: a pipeline "MS SQL Server=>PostgreSQL"

## Prerequisites

- Python 3.x (pandas, SQLAlchemy)
- PostgreSQL, SQL Server databases configured
- Install Airflow with Docker, or in a separate repo to avoid conflicts with the script modules

1. Download and configure databases
2. Load a data sample to a database
3. Extract data using python
4. Transform (optionally)
5. Load to another db
   Optionally: Schedule with Airflow

## Result
 
<img src="images/pandas_goal_achieved.png" alt="Data imported finally" />
<p align="center">
  <img src="images/pandas_rows_match_after_repl.png" alt="Each byte replicated" width="50%">
</p>
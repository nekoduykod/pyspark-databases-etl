## About  

PySpark ETL/EL

<img src="images/pyspark%20proj.jpg" />

## MySQL to PostgreSQL PySpark Pipeline

The scripts provide flexibility: ETL one - MySQL data extraction => aggregations => load results to PostgreSQL, another - EL, allowing transformations at the destination database (aka ELT)

## Prerequisites

- Python 3.x; run "pip install pyspark" within a virtual environment. I used Python 3.10, PySpark 3.4.1
- MySQL and PostgreSQL installed and configured. I obtained a dataset from Kaggle
- Download MySQL and PostgreSQL connector JAR files
- For Windows users: Choose Java 8 and download winutils.exe from [here](https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin)
- For Linux/macOS users: ensure compatibility with correct versions of Hadoop and Spark.
- Edit system env variables (JAVA_HOME, HADOOP_HOME)
- Consider disabling Windows Firewall, antivirus software, or create appropriate ingress rules

## Results

1. MySQL Dataset:   
<img src="images/MySQL%20imdb%20dataset.jpg" />

2. Data Loaded:
<img src="images/data%20imported.jpg" />

3. MySQL Table => PostgreSQL using PySpark:
<img src="images/MySQL%20table%20PySpark%20loaded%20to%20Postgre.jpg" />

4. SQL Query:
<img src="images/MySQL%20query%20for%20PySpark.jpg" />

5. Data with transformation imported:
<img src="images/Pyspark%20trsfmd%20MySQL%20to%20Postgre.jpg" />
<img src="images/MySQL%20table%20PySpark%20tfmd_%20to%20Postgre.jpg" />

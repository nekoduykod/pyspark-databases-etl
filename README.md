## About The Project

This PySpark ETL project was aimed to gain practice. Previous one was about a dataset replication between two databases using pandas, SQLAlchemy. While pandas is suitable for small datasets, for larger volumes like gigabytes and terabytes, Apache Spark becomes essential. Spark, an open-source distributed computing framework, handles extensive data processing.  PySpark, a Python API for Spark, marries Spark's power with Python's simplicity, making it a preferred choice for big data tasks.  
Python's importance in data engineering is undeniable. It is important to enhance Python proficiency.

<img src="images/pyspark%20proj.jpg" />

## MySQL to PostgreSQL PySpark Pipeline

The scripts provide flexibility: one utilizes PySpark to extract MySQL data, perform aggregations, and load results into PostgreSQL (ETL), while the other focuses on extraction and loading (EL), allowing transformations at the destination database (thus, ELT)

## Prerequisites

- Python 3.x; run "pip install pyspark" within a virtual environment. I used Python 3.10, PySpark 3.4.1
- MySQL and PostgreSQL installed and configured. I obtained a dataset from Kaggle
- Download MySQL and PostgreSQL connector JAR files
- For Windows users: Choose Java 8 and download winutils.exe from [here](https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin)
- For Linux/macOS users: Ensure compatibility with correct versions of Hadoop and Spark.
- Edit system environment variables (JAVA_HOME, HADOOP_HOME)
- Consider disabling Windows Firewall and any antivirus software or create appropriate ingress rules

## Result

After the efforts, the project was a success! Nailed it.

1. MySQL Dataset to be replicated with PySpark:   
<img src="images/MySQL%20imdb%20dataset.jpg" />

2. Data Successfully Loaded:
<img src="images/data%20imported.jpg" />

3. MySQL Table Loaded into PostgreSQL Using PySpark:
<img src="images/MySQL%20table%20PySpark%20loaded%20to%20Postgre.jpg" />

4. SQL Query to be integrated in PySpark script:
<img src="images/MySQL%20query%20for%20PySpark.jpg" />

5. Data with transformation imported successfully:
<img src="images/Pyspark%20trsfmd%20MySQL%20to%20Postgre.jpg" />
<img src="images/MySQL%20table%20PySpark%20tfmd_%20to%20Postgre.jpg" />

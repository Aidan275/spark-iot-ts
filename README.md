# spark-iot-ts
A timeseries library using apache spark, filodb and flint ts library for reading the iot data in project haystack format and processing it.

## Features of this project
Some of the features of this project are:
- Ease of reading IOT devices histories using metadata and time range using project-haystack format
- Helpers and utilities for time series data analytics and processing
- Temporal (preserving sorting by time) merge, joins and other operations using flint ts library
- Support for streaming, batch analytics and ad-hoc queries using Apache Spark and FiloDB (built on top of Apache Cassandra)

## Architecture
![spark-iot-ts architecture](docs/spark-filo-flint-arch.jpg?raw=true "Architecture of IOT data processing framework")


## Technology Stack
1. Apache Spark
2. FiloDB (and Apache Cassandra)
3. Zeppelin Notebook / Jupyter Notebook
4. Spark Thrift Server (mysql as hive metastore)
5. Elasticsearch



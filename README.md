# spark-iot-ts
A platform for IOT timeseries analytics using Apache Spark, FiloDB and Flint ts library

## Objective of project
A Big Data backed platform for analytics of timeseries IOT data in cloud.

## Features of this project
Some of the features of this project are:
- Ease of reading IOT devices histories using metadata and time range using project-haystack format
- Helpers and utilities for time series data analytics and processing
- Temporal (preserving sorting by time) merge, joins and other operations using flint ts library
- Support for streaming, batch analytics and ad-hoc queries using Apache Spark and FiloDB (built on top of Apache Cassandra)

## Architecture
![spark-iot-ts architecture](docs/spark-filo-flint-arch.jpg?raw=true "Architecture of IOT data processing framework")


## Technology Stack
1. [Apache Spark](#apache-spark)
2. [Apache Cassandra](#apache-cassandra)
3. [Elasticsearch](#elasticsearch)
4. [Apache Kafka](#apache-kafka)
5. [Spark Thrift Server](#spark-thrift-server)
6. [Zeppelin Notebook](#zeppelin-notebook)
7. [Superset](#superset)

### Apache Spark
[Apache Spark](https://spark.apache.org/) along with [FiloDB](https://github.com/filodb/FiloDB) library and
 [Flint ts](https://github.com/twosigma/flint) library is the core of this project.

> Apache Spark™ is a fast and general engine for large-scale data processing.

 Apache Spark can be used for batch processing, streaming, machine learning and graph analytics on data generated by iot devices.

> The ability to analyze time series data at scale is critical for the success of finance and IoT applications based on Spark. Flint is Two Sigma's implementation of highly optimized time series operations in Spark. It performs truly parallel and rich analyses on time series data by taking advantage of the natural ordering in time series data to provide locality-based optimizations.

 Flint ts library supports temporal (preserving sorting by time) joins, merges and other timeseries related operations like group by cycles (same timestamp), group by intervals, time based windows, etc.

 > FiloDB is a new open-source distributed, versioned, and columnar analytical database designed for modern streaming workloads.

 FiloDB uses Apache Cassandra as data storage and built around Apache Spark framework. There is flexibility of data partitioning, sorting while storing, fast read/write and versioned data storage which is optimized for timeseries and events data.

 Currently tested for: Apache Spark 2.0.2
 Libraries forked at:
 - https://github.com/itsmeccr/FiloDB (Branch: v0.4_spark2)
 - https://github.com/itsmeccr/flint

### Apache Cassandra
> The Apache Cassandra database is the right choice when you need scalability and high availability without compromising performance. Linear scalability and proven fault-tolerance on commodity hardware or cloud infrastructure make it the perfect platform for mission-critical data.Cassandra's support for replicating across multiple datacenters is best-in-class, providing lower latency for your users and the peace of mind of knowing that you can survive regional outages.

[Apache Cassandra](https://cassandra.apache.org/) is used as data storage (IOT histories) by FiloDB.

### Elasticsearch
> Elasticsearch is a distributed RESTful search engine built for the cloud.

[Elasticsearch](https://github.com/elastic/elasticsearch) is used for storing and querying IOT metadata in [project-haystack](https://project-haystack.org/) format.

### Apache Kafka
> Kafka® is used for building real-time data pipelines and streaming apps. It is horizontally scalable, fault-tolerant, wicked fast, and runs in production in thousands of companies.

[Apache Kafka](https://kafka.apache.org/) is used as buffer in streaming uses cases.

### Spark Thrift Server
Spark Thrift Server comes in Apache spark distribution and can be used to expose spark dataframes as SQL tables.
It is used to connect histories in FiloDB and metadata in elasticsearch with visualization tools and other applications using JDBC.

### Zeppelin Notebook
> Web-based notebook that enables data-driven, interactive data analytics and collaborative documents with SQL, Scala and more.

[Apache Zeppelin](https://zeppelin.apache.org) is used to write business logic and rules and perform other data analytics.

### Superset
> Apache Superset (incubating) is a modern, enterprise-ready business intelligence web application

[Apache Superset](https://github.com/apache/incubator-superset) is used as visualization tool by connecting with Spark thrift server.


## Schema

### Metadata Schema

Metadata is stored in elasticsearch, hence the rules or convention regarding elasticsearch mapping or field names should be followed.
The general idea is to ETL haystack format metadata from edge devices to elasticsearch in cloud.
In general, key-value pair will be used as tag or metadata.
The markers of haystack format will be changed to marker as field name and value as 1.
The elasticsearch type should be "metadata" (as convention) and the index should be aliased to "metadata".

Example 1 (Equipment Metadata):

```
{
"_index": "metadata_equip_v1",
"_type": "metadata",
"_id": "site_boiler_1",
"_score": 1,
"_source": {
"id": "site_boiler_1",
"equip": 1,
"boiler": 1,
"hvac": 1,
"capacity": 100,
"name": "Boiler 1",
"equipRef": "Site Boiler 1",
"levelRef": "level1",
"siteRef": "site"
}
}
```
Example 2 (Points Metadata):
```
{
"_index": "metadata_v2",
"_type": "metadata",
"_id": "OAH",
"_score": 1,
"_source": {
"hisSize": "27,292",
"mod": "11-Apr-2017 Tue 01:23:30 UTC",
"tz": "Sydney",
"air": 1,
"point": 1,
"dis": "OAH",
"analytics": 1,
"regionRef": "Western Corridor",
"his": 1,
"disMacro": "$equipRef $navName",
"imported": 1,
"humidity": 1,
"navName": "OAH",
"equipRef": "Site Building Info",
"id": "Site Building Info OAH",
"hisRollup": "max",
"hisEnd": "12-Oct-2017 Thu 07:00:01 AEDT",
"hisStart": "1-Jan-2017 Sun 00:15:00 AEDT",
"levelRef": "Site Plant",
"haystackConnRef": "DEMO_CLIENT",
"hisStatus": "ok",
"hisId": "84.095",
"kind": "Number",
"siteRef": "Site",
"hisEndVal": "67.959 %",
"unit": "%",
"haystackHis": "H.DEMO_ST.DEMO_Plant_ACU~242d1~2420OAH",
"outside": 1,
"sensor": 1
}
}
```

### Histories Schema
Following are the fields for history data.
timestamp, datetime, pointName and value are the timeseries data.
siteRef and yearMonth is for partition.
siteRef and equipName generally will be used as joining keys to get different points of same equipment.
```
 |-- pointName: string (nullable = false)
 |-- timestamp: timestamp (nullable = false)
 |-- datetime: long (nullable = false)
 |-- equipName: string (nullable = false)
 |-- siteRef: string (nullable = false)
 |-- unit: string (nullable = true)
 |-- yearMonth: string (nullable = false)
 |-- value: double (nullable = true)
```

Sample Data:
```
+--------------------+--------------------+-------------------+--------------+-------+----+---------+-----+
|           pointName|           timestamp|           datetime|     equipName|siteRef|unit|yearMonth|value|
+--------------------+--------------------+-------------------+--------------+-------+----+---------+-----+
|S.site.hayTest.Bool1|2018-02-02 00:19:...|1517530772000000000|S.site.hayTest| S.site|    |  2018-02|  0.0|
|S.site.hayTest.Bool1|2018-02-02 00:20:...|1517530803000000000|S.site.hayTest| S.site|    |  2018-02|  1.0|
|S.site.hayTest.Bool1|2018-02-02 00:20:...|1517530805000000000|S.site.hayTest| S.site|    |  2018-02|  0.0|
|S.site.hayTest.Bool1|2018-02-02 00:20:...|1517530811000000000|S.site.hayTest| S.site|    |  2018-02|  1.0|
|S.site.hayTest.Bool1|2018-02-02 00:20:...|1517530815000000000|S.site.hayTest| S.site|    |  2018-02|  0.0|
|S.site.hayTest.Bool1|2018-02-02 00:20:...|1517530824000000000|S.site.hayTest| S.site|    |  2018-02|  0.0|
+--------------------+--------------------+-------------------+--------------+-------+----+---------+-----+
```



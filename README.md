# spark-secondary-sort

The code from this repository presents two secondary sort implementations using pySpark:
1. using Spark groupByKey
2. using Spark repartitionAndSortWithinPartitions

**Dataset**

The applications use the taxi trips reported to the City of Chicago (https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew/data)
 in order to compute the total downtime of each taxi cab.
 
**Structure**

Each implementation has two pySpark apps:
 1. runs a secondary sort and just prints the sessionized data to disk
 2. runs a secondary sort, computes the total downtime of each taxi cab and prints the result to disk

The code is explained in detail in https://www.qwertee.io/blog/spark-secondary-sort/.

**Docker deployment**

The docker template used for the spark environment can be found at https://github.com/sebrestin/spark-docker-deployment.

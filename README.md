# modaclouds-hdb2ldb
This component is to transfer data from the History DB to the Locan DB.

To use it, first download and install Fuseki, which can be found in http://jena.apache.org/documentation/serving_data/
## How to run

```
java -jar hdb2ldb-<version>.jar hdbIP interval
```
where hdbIP is the IP of the Monitoring History DB and interval is the time interval to obtaining the data.

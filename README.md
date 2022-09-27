# Building

1. Make sure the Java 8 JDK and `sbt` are installed on your machine.
2. Export the `JAVA_HOME` environment variable with the path to the
JDK installation.
3. Run `build.sh`.

# Configuring the Migrator

Create a `config.yaml` for your migration using the template `config.yaml.example` in the repository root. Read the comments throughout carefully.

To use writetime filtering functionality uncomment the where: option under source on line 40 and add a filtercondition like writetime > 1664280262735767 and  writetime < 1664280266547453.

# Running locally

To run in the local Docker-based setup:

1. First start the environment:
```shell
docker-compose up -d
```

2. Launch `cqlsh` in Cassandra's container and create a keyspace and a table with some data:
```shell
docker-compose exec cassandra cqlsh
<create stuff>
```

3. Launch `cqlsh` in Scylla's container and create the destination keyspace and table with the same schema as the source table:
```shell
docker-compose exec scylla cqlsh
<create stuff>
```

4. Edit the `config.yaml` file; note the comments throughout.

5. Run `build.sh`.

6. Then, launch `spark-submit` in the master's container to run the job:
```shell
docker-compose exec spark-master /spark/bin/spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.scylla.config=/app/config.yaml \
  /jars/scylla-migrator-assembly-0.0.1.jar
```

The `spark-master` container mounts the `./target/scala-2.11` dir on `/jars` and the repository root on `/app`. To update the jar with new code, just run `build.sh` and then run `spark-submit` again.

# Writetime Filtering Code changes explaination

There were three main changes required to allow writetime filtering to take place.

1. Filtering based on the where option in the config file originally took place before the writetime and ttl columns were a part of the dataframe. Spark first pulls in just the data from the source, without any ttl or writetime. Then is the location of the original filter. Then it pulls in writetime information. Now the process is pull in data, pull in metadata, filter, which allows writetime and ttl based queries to be filtered on.

2. TTL type compatability. Originally, TTL values were traded as long type and int type in different various places within the code. When we tried to do certain specific things involving TTL it would try and fail to work on one data type as if it were the other, causing the migrator to crash. Now the treatment of TTLs is consistent throughout the code.

3. CassandraOption removal. CassandraOption is a class that allows the spark cassandra connector to skip over values that are missing or unset to save the time necesarry to actually tell the connector to transfer a null value. The options are None, Unset, and Value. Every field that was not a part of the priamry key was subject to this option. It cause values to be stored in the dataframe as Value(field) instead of just field. Cassandra Options cant be worked on the same as the Spark implicit types, so anything that actually needed to work on the dataframe in that state would cause crashes. The origninal scylla migrator code worked because it didn't need to mess with the datafram after that point, only transfer it. But with change #1, this also had to be removed befoure proper writetime filtering and transferral of data could take place.

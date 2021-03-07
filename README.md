# Scylla Migrator

This repo is a fork off the original [scylla-migrator](https://github.com/scylladb/scylla-migrator) and is for educational purposes. We have bypassed some steps like running `build.sh` to make it as fast as possible. If you want to go through the process of running the local Docker example Scylla provides or running the migrator on a live Spark cluster, you can find the instructions on their original repository linked above. Additionally, if you want to further extend this demo, follow the instructions on the original repository for how to rebuild the JAR after making code changes. 

## Prerequisites

1. [Docker](https://www.docker.com/)

## Demo

### 1. Clone this repo

```bash
git clone https://github.com/adp8ke/scylla-migrator.git
```

### 2. Get Docker containers started

If at any point you notice `137` errors and containers crash or refuse start, you will need to increase the memory resource allocation. You can do this by going to your Docker settings and increasing the memory from 2.00 GB to 4.00 GB (or higher if 4.00 doesn't work). I ran into these `137` issues and increasing my memory resources from 2.00 to 4.00 GB allowed this demo to work as needed.

```bash
cd scylla-migrator
docker-compose up -d
```

### 3. Add CQL files to Cassandra Containers

We will run a few commands to add the `cql` files that we will run onto the source and target Cassandra containers.

```bash
docker cp source.cql scylla-migrator_cass1_1:/
docker cp target.cql scylla-migrator_cass2_1:/
```

### 4. Set up Source and Target Cassandra Containers

1. Open a new terminal / tab and run the following commands for the Source Cassandra container

```bash
docker-compose exec cass1 cqlsh
```
then
```bash
source '/source.cql'
```
then
```bash
select count(*) from demo.spacecraft_journey_catalog ;
```
This should return 1000 rows. These are the 1000 rows that we will transfer to the Target Cassandra Container

2. Open a new terminal / tab and run the following commands for the Target Cassandra container

```bash
docker-compose exec cass2 cqlsh
```
then
```bash
source '/target.cql'
```
then
```bash
select count(*) from demo.spacecraft_journey_catalog ;
```
This should return 0 rows as we will populate this table using the migrator. Remember, the destination table must have the same schema as the source table. If you want to rename columns, you can do so in the config.yaml file; however, for the sake of our purposes, we are not doing that. 

### 5. Set up `config.yaml` file

1. Go back to the first terminal / tab and create the `config.yaml` file from `config.yaml.example`

```bash
mv config.yaml.example config.yaml
vim config.yaml
```

2. Edit ***source host*** to `cass1` and update keyspace and table values to `demo` and `spacecraft_journey_catalog`, respectively
3. Edit ***target type*** to `cassandra`, host to `cass2`, and update keyspace and table values to `demo` and `spacecraft_journey_catalog`, respectively
4. Escape and save

### 6. Run Spark Migrator

We are now ready to run the Spark Migrator using all of the setup we have done now. Once you run the below command, you may notice that there are some savepoint issues. At the time of writing this, I have not been able to debug it; however, it does not cause any functional issue as the migration will work as intended. More on savepoints can be found in the `config.yaml` / `config.yaml.example` files. In order to reduce walkthrough time, we have already provided the assembled JAR using sbt assembly. The `spark-master` container mounts the `./target/scala-2.11` dir on `/jars` and the repository root on `/app`, so you all you have to do is just copy and paste the below command to run the migrator for this example. As mentioned above, if you want to update the JAR with new code, you will need to re-run `build.sh` and then run `spark-submit` again.

***NOTE: Make sure you are still in the first terminal***

```bash
docker-compose exec spark-master /spark/bin/spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.scylla.config=/app/config.yaml \
  /jars/scylla-migrator-assembly-0.0.1.jar
```

### 7. Confirm Migration

To confirm the migration occured, run the following command in the `Target` Cassandra Container terminal / tab. If you try it right away, you may see the numbers increase as Spark is working, but it should fully complete in less than ~10-15 seconds. But once completed, there should be 1000 rows in the Target Cassandra Container.

```bash
select count(*) from demo.spacecraft_journey_catalog ;
```

And that will wrap up the walkthrough. With Scylla's Spark Migrator, we were able to migrate data from one Cassandra container to another. We could use actual host addresses to move data between Cassandra clusters, but for the purposes of a quick learning exercise, we just used Docker to simulate it.

## Additional Resources
[Live Recording with Walkthrough]()
[Accompanying Blog]()
[Accompanying SlideShare]()

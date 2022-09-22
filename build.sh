SCALA_VERSION=2.12.14

pushd spark-cassandra-connector
sbt clean package
sbt ++${SCALA_VERSION} assembly
popd

if [ ! -d "./lib" ]; then
    mkdir lib
fi

cp ./spark-cassandra-connector/connector/target/scala-2.12/spark-cassandra-connector-assembly-*.jar ./lib

sbt clean package
sbt ++${SCALA_VERSION} assembly
pushd spark-cassandra-connector
#sbt -Djava.io.tmpdir="$TMPDIR" ++2.12.11 assembly
sbt ++2.12.11 assembly
popd

if [ ! -d "./lib" ]; then
    mkdir lib
fi

### MAIN
cp ./spark-cassandra-connector/connector/target/scala-2.12/spark-cassandra-connector-assembly-*.jar ./lib
#sbt -Djava.io.tmpdir="$TMPDIR" ++2.12.11 assembly
sbt  ++2.12.11 assembly

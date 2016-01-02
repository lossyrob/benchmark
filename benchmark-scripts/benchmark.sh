#!/bin/sh

spark-submit \
  --class climate.cmd.Benchmark \
  --master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
  --conf spark.mesos.coarse=false \
  --conf spark.executor.memory=$MEMORY \
  --conf spark.executorEnv.SPARK_LOCAL_DIRS=$LOCAL_DIRS \
  --driver-library-path /usr/local/lib $JAR \
  --instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal \
  --layers "tasmax-rcp26-ccsm4:8,tasmax-rcp45-ccsm4:8"

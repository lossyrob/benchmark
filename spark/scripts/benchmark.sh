#!/bin/sh

spark-submit \
--class climate.cmd.Benchmark \
--master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=200g \
--conf spark.executorEnv.SPARK_LOCAL_DIRS="/media/ephemeral0,/media/ephemeral1" \
--driver-library-path /usr/local/lib /tmp/benchmark.jar \
--instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal \
--layers "tasmax-rcp26-ccsm4:8,tasmax-rcp45-ccsm4:8"

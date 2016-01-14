INGEST BENCHMARKS

Attempt 1:

```scala
spark-submit \
--class climate.ingest.NEXIngest \
--master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=50g \
--conf spark.mesos.extra.cores=30 \
--conf spark.executorEnv.SPARK_LOCAL_DIRS="/media/ephemeral0,/media/ephemeral1" \
--driver-library-path /usr/local/lib $JAR \
--crs EPSG:4326 --instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal \
--input s3n://$AWS_ID:$AWS_KEY@nex-bcsd-tiled-geotiff/rcp26/tasmax/CCSM4 \
--layerName tasmax-rcp26-ccsm4  --table tas --clobber true --s3PageSize 1000
```

TIMING: 256.372511 s

NUMBER OF NODES: 2

INSTANCE TYPES:
  - follower -- i2.2xlarge
  - master -- m3.large

REGION: us-west-1

Attempt 2:

```scala
spark-submit \
--class climate.ingest.NEXIngest \
--master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=40g \
--conf spark.mesos.extra.cores=30 \
--conf spark.executorEnv.SPARK_LOCAL_DIRS="/media/ephemeral0,/media/ephemeral1" \
--driver-library-path /usr/local/lib $JAR \
--crs EPSG:4326 --instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal \
--input s3n://$AWS_ID:$AWS_KEY@nex-bcsd-tiled-geotiff/rcp26/tasmax/CCSM4 \
--layerName tasmax-rcp26-ccsm4  --table tas --clobber true --s3PageSize 1000
```

TIMING: 221.509851 s

NUMBER OF NODES: 8

INSTANCE TYPES:
  - follower -- i2.2xlarge
  - master -- m3.large

REGION: us-west-1

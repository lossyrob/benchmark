#!/bin/sh

spark-submit \
--class climate.ingest.NEXIngest \
--master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
--conf spark.mesos.coarse=false \
--conf spark.executor.memory=$MEMORY \
--conf spark.executorEnv.SPARK_LOCAL_DIRS="$LOCAL_DIRS" \
--driver-library-path /usr/local/lib $JAR \
--crs EPSG:4326 --instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal \
--input s3n://$AWS_ID:$AWS_KEY@nex-bcsd-tiled-geotiff/$CLIMATE_MODEL/tasmax/CCSM4 \
--layerName tasmax-$CLIMATE_MODEL-ccsm4  --table tas --clobber true --s3PageSize 691

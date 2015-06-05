#!/bin/sh

spark-submit \
    --class climate.ingest.ParquetIngest \
    --master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
    --conf spark.mesos.coarse=true \
    --jars /tmp/hadoop-aws-2.6.0.jar \
    --conf spark.executor.memory=20g \
    --conf spark.executorEnv.SPARK_LOCAL_DIRS="/media/ephemeral0,/media/ephemeral1" \
    --conf spark.sql.parquet.output.committer.class=org.apache.spark.sql.parquet.DirectParquetOutputCommitter \
    --driver-library-path /usr/local/lib /tmp/benchmark.jar \
    --crs EPSG:4326 --input s3a://nex-bcsd-tiled-geotiff.azavea.com/rcp26/tasmax/CCSM4/*.tif \
    --output s3a://nex-parquet.spark.azavea.com --layerName tasmax-rcp26-ccsm4

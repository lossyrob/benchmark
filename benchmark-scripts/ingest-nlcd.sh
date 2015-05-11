#!/bin/sh

JAR=spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar

zip -d $JAR META-INF/ECLIPSEF.RSA
zip -d $JAR META-INF/ECLIPSEF.SF

export AWS_ID=AKIAJERCID77YLDHFBAQ
export AWS_KEY=d7Yu7pKl1f2XidwQYSMQQ5JNBY3pHnukmCmUMmaj

export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so

spark-submit \
--class geotrellis.spark.ingest.S3IngestCommand \
--master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=50g \
--conf spark.executorEnv.SPARK_LOCAL_DIRS="/media/ephemeral0,/media/ephemeral1" \
--driver-library-path /usr/local/lib spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar \
--input s3n://$AWS_ID:$AWS_KEY@gt-rasters/nlcd/2011/tiles-small \
--layerName nlcd --crs EPSG:3857 --clobber true \
--bucket gt-rasters --key catalog

#!/bin/sh

JAR=spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar

zip -d $JAR META-INF/ECLIPSEF.RSA
zip -d $JAR META-INF/ECLIPSEF.SF

AWS_ID=AKIAIPPGSEPLCCOAIESA
AWS_KEY=vm+az1/Yj+u3pfOXoZNbqp1AMGi9452TdMXiy+lw

spark-submit \
--class climate.ingest.NexHdfsIngest \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=50g \
--conf spark.local.dir="/media/ephemeral0,/media/ephemeral1" \
--driver-library-path /usr/local/lib spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar \
--input s3n://$AWS_ID:$AWS_KEY@nex-bcsd-tiled-geotiff/rcp26/pr/CCSM4/pr_amon_BCSD_rcp26_r1i1p1_CONUS_CCSM4_200601-201012-200601120000 \
--layerName pr-rcp26-ccsm4-2 --crs EPSG:3857 --clobber true \
--catalog hdfs://namenode.service.geotrellis-spark.internal/ingest

#!/bin/sh

JAR=spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar

zip -d $JAR META-INF/ECLIPSEF.RSA
zip -d $JAR META-INF/ECLIPSEF.SF
ID=AKIAJUDSJR2UADSMRTAA
KEY=l8ayXdBO5pug5No0Z4tP+8Y/B0QLzSvQeyv6UdV0

export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so

spark-submit \
--class climate.ingest.NEXIngest \
--master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=50g \
--conf spark.executorEnv.SPARK_LOCAL_DIRS="/media/ephemeral0,/media/ephemeral1" \
--driver-library-path /usr/local/lib spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar \
--crs EPSG:4326 --instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal \
--input s3n://$ID:$KEY@nex-bcsd-tiled-geotiff/rcp26/pr/CCSM4/pr_amon_BCSD_rcp26_r1i1p1_CONUS_CCSM4_200601-201012-200601120000 \
--layerName pr-rcp26-ccsm4-small  --table pr_small --clobber true  --s3PageSize 1000

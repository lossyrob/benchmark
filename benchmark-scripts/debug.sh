#!/bin/sh

JAR=spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar

zip -d $JAR META-INF/ECLIPSEF.RSA
zip -d $JAR META-INF/ECLIPSEF.SF


spark-submit \
--class climate.cmd.Debug \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=50g \
--conf spark.local.dir="/media/ephemeral0/spark,/media/ephemeral1/spark" \
--driver-library-path /usr/local/lib spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar \
--instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal \
--layers "tasmax-rcp45-ccsm4:8,tasmax-rcp26-ccsm4:8"

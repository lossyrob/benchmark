#!/bin/sh

JAR=spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar

zip -d $JAR META-INF/ECLIPSEF.RSA
zip -d $JAR META-INF/ECLIPSEF.SF

export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so

spark-submit \
--class climate.rest.CatalogService \
--master mesos://zk://zookeeper.service.geotrellis-spark.internal:2181/mesos \
--conf spark.mesos.coarse=true \
--conf spark.executor.memory=50g \
--conf spark.executorEnv.SPARK_LOCAL_DIRS="/media/ephemeral0,/media/ephemeral1" \
--driver-library-path /usr/local/lib spark/target/scala-2.10/geotrellis-spark-assembly-0.10.0-SNAPSHOT.jar \
--instance geotrellis-accumulo-cluster --user root --password secret --zookeeper zookeeper.service.geotrellis-spark.internal

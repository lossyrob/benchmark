package climate.ingest

import geotrellis.spark._
import geotrellis.spark.ingest.NetCDFIngestCommand._
import geotrellis.spark.tiling._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.s3._
import geotrellis.spark.ingest._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.utils.SparkUtils
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark._
import com.quantifind.sumac.ArgMain
import com.github.nscala_time.time.Imports._
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.quantifind.sumac.validation.Required

class NexIngestArgs extends AccumuloIngestArgs {
  @Required var s3PageSize: Integer = _
}

/** Ingests the chunked NEX GeoTIFF data */
object NEXIngest extends ArgMain[NexIngestArgs] with LazyLogging {
  def main(args: NexIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = SparkUtils.createSparkContext("Ingest")
    val job = sparkContext.newJob

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val layoutScheme = ZoomedLayoutScheme()

    // Get source tiles
    val inPath = args.inPath
    S3InputFormat.setUrl(job, inPath.toUri.toString)
    S3InputFormat.setMaxKeys(job, args.s3PageSize)
    val source =
      sparkContext.newAPIHadoopRDD(
        job.getConfiguration,
        classOf[TemporalGeoTiffS3InputFormat],
        classOf[SpaceTimeInputKey],
        classOf[Tile]
      )

    Ingest[SpaceTimeInputKey, SpaceTimeKey](source, args.destCrs, layoutScheme){ (rdd, level) =>
      accumulo.catalog.save(LayerId(args.layerName, level.zoom), args.table, rdd, args.clobber)
    }
  }
}

package climate.cmd

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.ingest.{Ingest, Pyramid, HadoopIngestArgs}
import geotrellis.spark.io.hadoop.formats._
import geotrellis.spark.utils.SparkUtils
import geotrellis.spark.io.index._

import org.apache.spark._
import com.quantifind.sumac.ArgMain

/**
 * Ingests raw multi-band NetCDF tiles into a re-projected and tiled RasterRDD
 */
object HDFSIngest extends ArgMain[HadoopIngestArgs] with Logging {
  def main(args: HadoopIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = SparkUtils.createSparkContext("Ingest")
    val conf = sparkContext.hadoopConfiguration
    conf.set("io.map.index.interval", "1")

    val source = sparkContext.netCdfRDD(args.inPath).repartition(12);
    val layoutScheme = ZoomedLayoutScheme(256)
    Ingest[NetCdfBand, SpaceTimeKey](source, args.destCrs, layoutScheme, true){ (rdd, level) =>
      val catalog = HadoopRasterCatalog(args.catalogPath)
      catalog.writer[SpaceTimeKey](ZCurveKeyIndexMethod.byYear).write(LayerId(args.layerName, level.zoom), rdd)
    }
  }
}

package climate.ingest

import geotrellis.spark._
import geotrellis.spark.ingest.NetCDFIngestCommand._
import climate.cmd._

import geotrellis.spark.tiling._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.json._

import scala.collection.mutable

import geotrellis.spark.ingest._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.utils.{SparkUtils, KryoSerializer}
import geotrellis.raster.io.geotiff.reader._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark._
import com.quantifind.sumac.ArgMain
import com.github.nscala_time.time.Imports._
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.quantifind.sumac.validation.Required
import geotrellis.spark.io.index._
import org.apache.spark.storage.StorageLevel

import spray.json._
import DefaultJsonProtocol._

import scala.collection.mutable._
import spire.syntax.cfor._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}


case class TileRow(tileRow: Integer, tileCol:Integer, coords: (Integer, Integer),
  year: Integer, month: Integer, day: Integer, timeMillis: Long,
  zIndex: Long, tile: Array[Byte])

case class RMD(rasterMetaData: String, keyBounds: String)

class ParquetArgs extends IngestArgs {
  @Required var output: String = _
}

object ParquetRasterMetaDataReader {

  def read(path: String)(implicit sqlContext: SQLContext): scala.collection.Map[String,
    (RasterMetaData, KeyBounds[SpaceTimeKey])] = {

    val rasterMetaDataRDD = sqlContext.load(path, "parquet").rdd.map{ case Row(rmd, kb, zoomLevel, layerName) =>
      val rasterMetaData = rmd.asInstanceOf[String].parseJson.convertTo[RasterMetaData]
      val keyBounds = kb.asInstanceOf[String].parseJson.convertTo[KeyBounds[SpaceTimeKey]]
      val key = s"$layerName:$zoomLevel"
      (key, (rasterMetaData, keyBounds))
    }
    rasterMetaDataRDD.collectAsMap
  }

}

object ParquetIngest extends ArgMain[ParquetArgs] with LazyLogging {

  def ingestSpaceTimeKey(sc: SparkContext, sqlContext: SQLContext, args: ParquetArgs): Unit = {

    import sqlContext.implicits._

    val baseOutput = s"${args.output}/spacetime/"

    val sourceTiles = sc.binaryFiles(args.input).map{ case(inputKey, b) =>
      val geoTiff = GeoTiffReader.read(b.toArray)
      val meta = geoTiff.metaData
      val isoString = geoTiff.tags("ISO_TIME")
      val dateTime = DateTime.parse(isoString)
      val GeoTiffBand(tile, extent, crs, _) = geoTiff.bands.head
      (SpaceTimeInputKey(extent, crs, dateTime), tile)
    }

    val destCrs = args.destCrs

    val reprojectedTiles = sourceTiles.reproject(destCrs)
    val layoutScheme = ZoomedLayoutScheme()
    val isUniform = false

    var (layoutLevel, rasterMetaData) =
      RasterMetaData.fromRdd(reprojectedTiles, destCrs, layoutScheme, isUniform) { key =>
        key.projectedExtent.extent
      }

    val rasterRdd = SpaceTimeInputKey.tiler(reprojectedTiles, rasterMetaData)
    rasterRdd.persist(StorageLevel.MEMORY_AND_DISK)
    val keyBounds = implicitly[Boundable[SpaceTimeKey]].getKeyBounds(rasterRdd)
    val keyIndexMethod = ZCurveKeyIndexMethod.by( (d: DateTime) => 0)

    val imin = keyBounds.minKey.updateSpatialComponent(SpatialKey(0, 0))
    val imax = keyBounds.maxKey.updateSpatialComponent(
      SpatialKey(rasterMetaData.tileLayout.layoutCols - 1,
        rasterMetaData.tileLayout.layoutRows - 1))
    val indexKeyBounds = KeyBounds(imin, imax)
    val keyIndex = keyIndexMethod.createIndex(indexKeyBounds)

    val layerId = LayerId(args.layerName, 8)

    Timer.timedTask("Saving Parquet Raster Tiles: STK", s => logger.info(s)) {
      val rasterDFBinary = rasterRdd.map{case (k, t) =>
        val bytes = KryoSerializer.serialize[Tile](t)
        TileRow(k.row, k.col, (k.row, k.col), k.time.getYear, k.time.getMonthOfYear,
          k.time.getDayOfMonth, k.time.getMillis, keyIndex.toIndex(k), bytes)
      }.toDF().coalesce(20).orderBy("zIndex")
      val numObs = rasterDFBinary.count()
      logger.info(s"NUMBER OF OBSERVATIONS: $numObs")
      val climateRasterOutStr = s"$baseOutput/rasterData/zoomLevel=${layerId.zoom}/layerName=${layerId.name}/"
      rasterDFBinary.write.format("parquet").save(climateRasterOutStr)
    }

    Timer.timedTask("Saving RasterMetaData: STK", s => logger.info(s)) {
      val climateRMDOutStr = s"$baseOutput/rasterMetaData/zoomLevel=${layerId.zoom}/layerName=${layerId.name}/"
      val rmdDF = sc.parallelize(Seq(RMD(rasterMetaData.toJson.toString, keyBounds.toJson.toString))).toDF()
      rmdDF.save(climateRMDOutStr, "parquet")
    }
  }

  def main(args: ParquetArgs): Unit = {

    val sc = SparkUtils.createSparkContext("Parquet-Ingest")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class",
      "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")

    ingestSpaceTimeKey(sc, sqlContext, args)
  }
}

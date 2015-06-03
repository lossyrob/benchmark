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

// case class TileRow(index: Long, row: Integer, col:Integer,
//   timeMillis: Long, tile: Array[Byte])

case class TileRow(row: Integer, col:Integer,
  timeMillis: Long, zIndex: Long, tile: Array[Byte])

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

  // def test: Unit ={

  //   val mapRmd = read(sqlContext, "/home/cbrown/Documents/rasterMetaData/")

  // }

}



// Get dataframes which will have row, col, time (millis), tile
object ParquetRasterReader extends LazyLogging {

  def getFilterRanges(filterSet: FilterSet[SpaceTimeKey], keyBounds: KeyBounds[SpaceTimeKey],
    keyIndex: KeyIndex[SpaceTimeKey]): Seq[(Long, Long)] = {

    val spaceFilters = mutable.ListBuffer[GridBounds]()
    val timeFilters = mutable.ListBuffer[(DateTime, DateTime)]()

    filterSet.filters.foreach {
      case SpaceFilter(bounds) =>
        spaceFilters += bounds
      case TimeFilter(start, end) =>
        timeFilters += ( (start, end) )
    }

    if(spaceFilters.isEmpty) {
      val minKey = keyBounds.minKey.spatialKey
      val maxKey = keyBounds.maxKey.spatialKey
      spaceFilters += GridBounds(minKey.col, minKey.row, maxKey.col, maxKey.row)
    }

    if(timeFilters.isEmpty) {
      val minKey = keyBounds.minKey.temporalKey
      val maxKey = keyBounds.maxKey.temporalKey
      timeFilters += ( (minKey.time, maxKey.time) )
    }

    (for {
      bounds <- spaceFilters
      (timeStart, timeEnd) <- timeFilters
    } yield {
      keyIndex.indexRanges(
        SpaceTimeKey(bounds.colMin, bounds.rowMin, timeStart),
        SpaceTimeKey(bounds.colMax, bounds.rowMax, timeEnd))
    }).flatten
  }

  def applyFilterRanges(df: DataFrame, filterRanges: Seq[(Long, Long)]): DataFrame = {

    def applyFilterRange(df: DataFrame, floor: Long, ceiling: Long): DataFrame =
      df.filter(df("zIndex") >= floor).filter(df("zIndex") <= ceiling)

    filterRanges match {
      case Seq() => df
      case (floor, ceiling) +: tail => applyFilterRanges(applyFilterRange(df, floor, ceiling), tail)
    }

  }

  def readRasterZCurve(path: String, keyBounds: KeyBounds[SpaceTimeKey],
    rasterMetaData: RasterMetaData, polygon: Polygon)(implicit sqlContext: SQLContext): DataFrame = {

    // Create filterset for grid bounds
    logger.info("Getting Grid Bounds")
    val filterGridBounds = rasterMetaData.mapTransform(polygon.envelope)

    logger.info("ConstructingFilterSet")
    val filterSet = FilterSet(SpaceFilter[SpaceTimeKey](filterGridBounds))

    val keyIndex = ZCurveKeyIndexMethod.byYear.createIndex(keyBounds)
    // Get filter ranges

    logger.info("Getting Filter Ranges")
    val stkFilterRanges = getFilterRanges(filterSet, keyBounds, keyIndex)

    logger.info("Reading DataFrame")
    val df = sqlContext.load(path, "parquet")

    logger.info(s"Applying Filter Ranges: ${stkFilterRanges.size}")
    applyFilterRanges(df, stkFilterRanges)
  }

  def readRasterRowsCols(path: String, keyBounds: KeyBounds[SpaceTimeKey],
    rasterMetaData: RasterMetaData, polygon: Polygon)(implicit sqlContext: SQLContext): DataFrame = {

    // Create filterset for grid bounds
    logger.info("Getting Grid Bounds")
    val filterGridBounds = rasterMetaData.mapTransform(polygon.envelope)

    logger.info("ConstructingFilterSet")
    val df = sqlContext.load(path, "parquet")

    df.filter(df("row") <= filterGridBounds.rowMax)
      .filter(df("row") >= filterGridBounds.rowMin)
      .filter(df("col") <= filterGridBounds.colMax)
      .filter(df("col") >= filterGridBounds.colMin)
  }


  def readSpaceTimeKeys(path: String, filterRanges: Seq[(Long, Long)]):
      RasterRDD[(SpaceTimeKey, Tile)] = ???

  def readSpatialKeysRaster(path: String, filterRanges: Seq[(Long, Long)]):
      RasterRDD[(SpaceTimeKey, Tile)] = ???

}

object ReadTimings extends LazyLogging {

  import Extents._

  def testReads(p: String)(implicit sqlContext: SQLContext): Unit = {

    val mdPath = s"$p/rasterMetaData"
    val rasterPath = s"$p/rasterData"

    val md = ParquetRasterMetaDataReader.read(mdPath)
    val (rmd, keyBounds) = md("one-month-test:8")

    val ek = extents("USA")

    Timer.timedTask("DFTEST: Loading Zcurve eastKansas", s => logger.info(s)) {
      val zdf = ParquetRasterReader.readRasterZCurve(rasterPath, keyBounds, rmd, ek)
      // val c = zdf.count
      // logger.info(s"DFTEST: ZCURVE COUNT: $c")
    }

    // Timer.timedTask("DFTEST: Loading Row & Column eastKansas", s => logger.info(s)) {
    //   val zdf = ParquetRasterReader.readRasterRowsCols(rasterPath, keyBounds, rmd, ek)
    //   val c = zdf.count
    //   zdf.rdd.first
    //   logger.info(s"DFTEST: ROW & COLUMN COUNT: $c")
    // }

  }

}



case class TileCell(tileRow: Integer, tileCol: Integer, timeMillis: Long,
  row: Integer, col: Integer, value: Double)

object ParquetIngest extends ArgMain[ParquetArgs] with LazyLogging {

  def tile2cells(k:SpaceTimeKey, t:Tile): MutableList[TileCell] = {
    val height = t.rows
    val width = t.cols

    var s = MutableList[TileCell]()

    cfor(0)(_ < height, _ + 1) { row =>
      cfor(0)(_ < width, _ + 1) { col =>
        val cellValue = t.getDouble(row, col)
        if (!cellValue.isNaN) {
          s += TileCell(k.row, k.col, k.time.getMillis, row, col, cellValue)
        }
      }
    }
    s
  }

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
    val keyIndexMethod = ZCurveKeyIndexMethod.byYear

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
        TileRow(k.row, k.col, k.time.getMillis, keyIndex.toIndex(k), bytes)
      }.toDF().coalesce(20).orderBy("zIndex")
      val numObs = rasterDFBinary.count()
      logger.info(s"NUMBER OF OBSERVATIONS: $numObs")
      val climateRasterOutStr = s"$baseOutput/rasterData/zoomLevel=${layerId.zoom}/layerName=${layerId.name}/"
      rasterDFBinary.write.format("parquet").save(climateRasterOutStr)
    }

    // Timer.timedTask("Saving Raster Cell DataFrame: STK", s => logger.info(s)) {
    //   val rasterCellDF = rasterRdd.flatMap{ case (k, t) =>
    //     tile2cells(k, t)
    //   }.toDF().orderBy("zIndex")
    //   val climateRasterCellOutStr =
    //     s"$baseOutput/rasterCellData/zoomLevel=${layerId.zoom}/layerName=${layerId.name}/"
    //   rasterCellDF.save(climateRasterCellOutStr, "parquet")
    // }

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

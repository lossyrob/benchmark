package climate.cmd

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.FieldArgs
import com.quantifind.sumac.validation.Required
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.cmd.args._
import geotrellis.spark.ingest.IngestArgs
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.index._
import geotrellis.spark.op.stats._
import scala.collection.mutable
import geotrellis.spark.utils.SparkUtils
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.reproject._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark._
import geotrellis.spark.utils.{SparkUtils, KryoSerializer}
import geotrellis.spark.io.hadoop.KryoRegistrator
import geotrellis.vector.io.json._
import geotrellis.spark.op.zonal.summary._
import geotrellis.raster.op.zonal.summary._
import geotrellis.spark.op.stats._
import com.github.nscala_time.time.Imports._
import geotrellis.raster.op.local
import geotrellis.raster._
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.github.nscala_time.time.Imports._
import geotrellis.spark.op.local._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.storage.StorageLevel
import spray.json._
import geotrellis.spark.io.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.hive.HiveContext
import geotrellis.raster.op.local._
import scala.math._

object ParquetRasterMetaDataReader {

  def read(path: String, sqlContext: SQLContext): scala.collection.Map[String,
    (RasterMetaData, KeyBounds[SpaceTimeKey])] = {

    val rasterMetaDataRDD = sqlContext.load(path, "parquet").rdd.map{ case Row(rmd, kb, zoomLevel, layerName) =>
      val rasterMetaData = rmd.asInstanceOf[String].parseJson.convertTo[RasterMetaData]
      val keyBounds = kb.asInstanceOf[String].parseJson.convertTo[KeyBounds[SpaceTimeKey]]
      val key = s"$layerName:$zoomLevel"
      (key, (rasterMetaData, keyBounds))
    }
    rasterMetaDataRDD.collectAsMap
  }

  def readLayerMetaData(path: String, layerId: LayerId, sqlContext: SQLContext):
      (RasterMetaData, KeyBounds[SpaceTimeKey]) = {
    val metaData = read(path, sqlContext)
    val metaDataKey = s"${layerId.name}:${layerId.zoom}"
    metaData(metaDataKey)
  }

}


object ParquetRasterReader extends LazyLogging {


  // Lifted from S3 Reader
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

    val keyIndex = ZCurveKeyIndexMethod.by( (d: DateTime) => 1).createIndex(keyBounds)
    // Get filter ranges

    logger.info("Getting Filter Ranges")
    val stkFilterRanges = getFilterRanges(filterSet, keyBounds, keyIndex)

    logger.info("Reading DataFrame")
    val df = sqlContext.load(path, "parquet")

    logger.info(s"Applying Filter Ranges: ${stkFilterRanges.size}")
    applyFilterRanges(df, stkFilterRanges)
  }

  def getGroup(i: Integer): Integer = {
    (i / 10) * 10
  }

  def readRasterRowsCols(df: DataFrame, layerId: LayerId,
    rasterMetaData: RasterMetaData, polygon: Polygon): DataFrame = {

    // Create filterset for grid bounds
    logger.info("Getting Grid Bounds")
    val filterGridBounds = rasterMetaData.mapTransform(polygon.envelope)

    val maxColGroup = getGroup(filterGridBounds.colMax)
    val minColGroup = getGroup(filterGridBounds.colMin)
    val maxRowGroup = getGroup(filterGridBounds.rowMax)
    val minRowGroup = getGroup(filterGridBounds.rowMin)

    df.filter(df("tileRow") <= filterGridBounds.rowMax)
      .filter(df("tileRow") >= filterGridBounds.rowMin)
      .filter(df("tileCol") <= filterGridBounds.colMax)
      .filter(df("tileCol") >= filterGridBounds.colMin)
      .filter(df("rowGroup") <= maxRowGroup)
      .filter(df("rowGroup") >= minRowGroup)
      .filter(df("colGroup") <= maxColGroup)
      .filter(df("colGroup") >= minColGroup)
      .filter(df("layerName") === layerId.name)
  }

}


class ParquetBenchmarkArgs extends FieldArgs {
  /** Comma seprated list of layerId:Zoom */
  @Required var layers: String = _

  @Required var input: String = _

  def getLayers: Array[LayerId] =
    layers
      .split(",")
      .map{ str =>
        val Array(name, zoom) = str.split(":")
        LayerId(name, zoom.toInt)
      }
}


object ParquetBenchmark extends ArgMain[ParquetBenchmarkArgs] with LazyLogging {
  import Extents._

  def getDataFrame(
    df: DataFrame,
    id: LayerId,
    polygon: Polygon,
    metaData: org.apache.spark.broadcast.Broadcast[scala.collection.Map[String,
        (RasterMetaData, KeyBounds[SpaceTimeKey])]]
  ): DataFrame = {
    val metaDataKey = s"${id.name}:${id.zoom}"
    val (rmd, keyBounds) = metaData.value(metaDataKey)
    println(s"getRDD RMD: $rmd")
    val bounds = rmd.mapTransform(polygon.envelope)

    println(s"getRDD GridBounds: $bounds")
    ParquetRasterReader.readRasterRowsCols(df, id, rmd, polygon)
  }

  def stats(rdd: RasterRDD[SpaceTimeKey]): String = {
    val crdd = rdd.cache()
    val tiles = rdd.count
    val cells = tiles * rdd.metaData.tileLayout.tileSize
    crdd.unpersist()
    s"tiles=$tiles, cells=$cells"
  }

  def subtractTiles(tile1bytes: Array[Byte], tile2bytes: Array[Byte]): Array[Byte] = {
    val tile1 = KryoSerializer.deserialize[Tile](tile1bytes)
    val tile2 = KryoSerializer.deserialize[Tile](tile2bytes)
    val subtractedTiles = tile1 - tile2
    KryoSerializer.serialize[Tile](subtractedTiles)
  }

  def main(args: ParquetBenchmarkArgs): Unit = {
    val sparkContext = SparkUtils.createSparkContext("Benchmark")

    sparkContext.hadoopConfiguration.set("spark.sql.parquet.output.committer.class",
      "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")

    // sparkContext.hadoopConfiguration.set("fs.defaultFS", "s3n://nex-parquet.spark.azavea.com")
    // sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJOVCA22UTHU7HNXQ")
    // sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
    sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "AKIAJOVCA22UTHU7HNXQ")
    sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", "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")

    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._

    // sparkContext.hadoopConfiguration.set("fs.defaultFS", "s3a://nex-parquet.spark.azavea.com")

    val Array(layer1, layer2) = args.getLayers

    val metaDataPath = s"${args.input}/rasterMetaData"

    val metaDatavalue = ParquetRasterMetaDataReader.read(metaDataPath, sqlContext)
    val metaData = sparkContext.broadcast(metaDatavalue)

    val rasterDataPath = s"${args.input}/rasterData"
    logger.info(s"RASTERDATA: $rasterDataPath")
    val df = sqlContext.load(rasterDataPath, "parquet")


    for {
      (name, polygon) <- extents
    } {
      val df1 = getDataFrame(df, layer1, polygon, metaData)
      df1.persist(StorageLevel.MEMORY_AND_DISK)
      val df2 = getDataFrame(df, layer2, polygon, metaData)
      df2.persist(StorageLevel.MEMORY_AND_DISK)
      df1.registerTempTable("df1")
      df2.registerTempTable("df2")

      val dfunion = df1.unionAll(df2)
      dfunion.registerTempTable("dfunion")

      val rootPath = "/home/cbrown/Documents/test-tifs"
      Timer.timedTask(s"""Benchmark: {type: Count 2 Layers, name: $name""", s=> logger.info(s)) {
        val x = df1.count()
        val y = df2.count()
        logger.info(s"LAYER: $name, COUNT: $x")
        logger.info(s"LAYER: $name, COUNT: $y")
      }

      Timer.timedTask(s"""Benchmark: {type: Join 2 Layers, name: $name}""", s=> logger.info(s)) {

        sqlContext.udf.register("subtractTiles",
          (tile1bytes: Array[Byte], tile2bytes: Array[Byte]) => {
            val tile1 = KryoSerializer.deserialize[Tile](tile1bytes)
            val tile2 = KryoSerializer.deserialize[Tile](tile2bytes)
            val subtractedTiles = tile1 - tile2
            val subtractTilesBytes = KryoSerializer.serialize[Tile](subtractedTiles)
            subtractTilesBytes
          })
        val result = sqlContext.sql("""SELECT df1.tileRow, df1.tileCol, subtractTiles(df1.tile, df2.tile) as tile
                        from df1, df2 WHERE df1.coords = df2.coords""")

        // result.write.format("parquet").save(s"$rootPath/subtract/layerName=$name/")

        logger.info(s"COUNT: ${result.count}")
      }

      Timer.timedTask(s"""Benchmark: {type: Average Yearly, name: $name}""", s=> logger.info(s)) {

        sqlContext.udf.register("averageTiles", (arrayTileBytes: mutable.ArrayBuffer[Array[Byte]]) => {
          val tiles = arrayTileBytes.map(t => KryoSerializer.deserialize[Tile](t))
          val startTile = tiles.head
          val sumTile = tiles.tail.foldLeft(startTile)(_ + _)
          val averageTile = sumTile / tiles.length
          KryoSerializer.serialize[Tile](averageTile)
        })

        val result = sqlContext.sql("""select tileRow, tileCol from dfunion GROUP BY tileRow, tileCol, year""")
        logger.info(s"YEARLY AVERAGE COUNT: ${result.count}")
        // result.write.format("parquet").save(s"$rootPath/yearlyAvg/layerName=$name/")
      }

    }
  }
}

package climate.cmd

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation.Required
import geotrellis.spark._
import geotrellis.spark.cmd.args._
import geotrellis.spark.ingest.IngestArgs
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.index._
import geotrellis.spark.op.stats._
import geotrellis.spark.utils.SparkUtils
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.reproject._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.apache.spark._
import geotrellis.vector.io.json._
import geotrellis.spark.op.zonal.summary._
import geotrellis.raster.op.zonal.summary._
import geotrellis.spark.op.stats._
import com.github.nscala_time.time.Imports._
import geotrellis.raster.op.local
import geotrellis.raster._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext._
import com.github.nscala_time.time.Imports._
import geotrellis.spark.op.local._

class BenchmarkArgs extends AccumuloArgs {
  /** Comma seprated list of layerId:Zoom */
  @Required var layers: String = _

  def getLayers: Array[LayerId] =
    layers
      .split(",")
      .map{ str =>
        val Array(name, zoom) = str.split(":")
        LayerId(name, zoom.toInt)
      }
}

object Extents extends GeoJsonSupport {
  import spray.json._
  val extents = Map[String, Polygon](
    "philadelphia" ->
      """{"type":"Feature","properties":{"name":6},"geometry":{
        "type":"Polygon",
        "coordinates":[[
          [-75.2947998046875,39.863371338285305],
          [-75.2947998046875,40.04023218690448],
          [-74.9432373046875,40.04023218690448],
          [-74.9432373046875,39.863371338285305],
          [-75.2947998046875,39.863371338285305]]]}}""".parseJson.convertTo[Polygon],
    "eastKansas" ->
      """{"type":"Feature","properties":{"name":9},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-98.26171875,37.055177106660814],
            [-98.26171875,39.9434364619742],
            [-94.6142578125,39.9434364619742],
            [-94.6142578125,37.055177106660814],
            [-98.26171875,37.055177106660814]]]}}""".parseJson.convertTo[Polygon],
    "Rockies" ->
      """{"type":"Feature","properties":{"name":3},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-120.23437499999999,32.69746078939034],
            [-120.23437499999999,48.19643332981063],
            [-107.9296875,48.19643332981063],
            [-107.9296875,32.69746078939034],
            [-120.23437499999999,32.69746078939034]]]}}""".parseJson.convertTo[Polygon],
    "USA" ->
      """{"type":"Feature","properties":{"name":3},"geometry":{
          "type":"Polygon",
          "coordinates":[[
            [-124.9132294655,25.6804735519],
            [-124.9132294655,49.2204934537],
            [-66.6759185791,49.2204934537],
            [-66.6759185791,25.6804735519],
            [-124.9132294655,25.6804735519]]]}}""".parseJson.convertTo[Polygon]
  )
}

object Benchmark extends ArgMain[BenchmarkArgs] with LazyLogging {
  import Extents._

  def getRdd(
    catalog: AccumuloRasterCatalog,
    id: LayerId,
    name: String
  ): RasterRDD[SpaceTimeKey] = {
    val rdd = catalog.reader[SpaceTimeKey].read(id)
    rdd.setName(name)
    rdd
  }

  def getRdd(
    catalog: AccumuloRasterCatalog,
    id: LayerId,
    polygon: Polygon,
    name: String
  ): RasterRDD[SpaceTimeKey] = {
    val lmd = catalog.attributeStore.read[AccumuloLayerMetaData](id, "metadata")
    val rmd = lmd.rasterMetaData
    println(s"getRDD RMD: $rmd")
    val bounds = rmd.mapTransform(polygon.envelope)
    println(s"getRDD GridBounds: $bounds")
    // val rdd = catalog.read[SpaceTimeKey].read(id, FilterSet(SpaceFilter[SpaceTimeKey](bounds)))
    val rdd = catalog.query[SpaceTimeKey](id).where(Intersects(bounds)).toRDD
    rdd.setName(name)
    rdd
  }

  def zonalSummary(rdd: RasterRDD[SpaceTimeKey], polygon: Polygon) = {
    rdd
      .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
      .averageByKey
      .zonalSummaryByKey(polygon, Double.MinValue, MaxDouble, stk => stk.temporalComponent.time)
      .collect
      .sortBy(_._1)
  }

  def annualAverage(rdd: RasterRDD[SpaceTimeKey]): Seq[(org.joda.time.DateTime, Double)] = {
    rdd
      .map{ case (key, tile) =>
        var total: Double = 0
        var count = 0L
        tile.foreachDouble{ d =>
          if(! isNoData(d)) {
            total += d
            count += 1
          }
        }
        val year = key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)).temporalComponent.time
        year -> (total, count)
      }
      .reduceByKey{ (tup1, tup2) =>
        (tup1._1 + tup2._1) -> (tup1._2 + tup2._2)
      }
      .collect
      .map { case (year, (sum, count)) =>
        year -> sum/count
      }
  }

  def stats(rdd: RasterRDD[SpaceTimeKey]): String = {
    val crdd = rdd.cache()
    val tiles = rdd.count
    val cells = tiles * rdd.metaData.tileLayout.tileSize
    crdd.unpersist()
    s"tiles=$tiles, cells=$cells"
  }


  def main(args: BenchmarkArgs): Unit = {
    implicit val sparkContext = SparkUtils.createSparkContext("Benchmark")
    implicit val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = AccumuloRasterCatalog("metadata")
    val layers = args.getLayers

    for {
      (name, polygon) <- extents
    } {
      def rdd1 = getRdd(catalog, layers(0), polygon, name)
      def rdd2 = getRdd(catalog, layers(1), polygon, name)

      Timer.timedTask(s"""Benchmark: {type: SingleModel-MinMax, name: $name }""", s=> logger.info(s)) {
        val (min, max) = rdd1.minMax

        logger.info(s"SINGLE MODEL MIN MAX: ${(min, max)}")
      }

      Timer.timedTask(s"""Benchmark: {type: SingleModel-Average, name: $name }""", s=> logger.info(s)) {
        val avg = annualAverage(rdd1)

        logger.info(s"SINGLE MODEL YEARLY AVERAGE OF DIFFERENCES ${avg}")
      }

      Timer.timedTask(s"""Benchmark: {type: MultiModel-localSubtract-Average, name: $name, layers: ${layers.toList}}""", s=> logger.info(s)) {
        val diff = rdd2 localSubtract rdd1
        val avg = annualAverage(diff)

        logger.info(s"MULTI MODEL AVERAGE ${avg}")
      }

      Timer.timedTask(s"""Benchmark: {type: MultiModel-localMean-MinMax, name: $name, layers: ${layers.toList}}""", s=> logger.info(s)) {
        val diff = Seq(rdd1, rdd2) localMean
        val minMax = diff.minMax

        logger.info(s"MUTLI MODEL MIN MAX ${minMax}")
      }

      Timer.timedTask(s"""Benchmark: {type: MultiModel-localSubtract-count, name: $name, layers: ${layers.toList}}""", s => logger.info(s)) {
        val diff = rdd2 localSubtract rdd1
        val count = diff.count

        logger.info(s"LOCAL SUBTRACT RECORD COUNT: ${count}")
      }

      Timer.timedTask(s"""Benchmark: {type: MultiModel-localSubtract-ZonalSummary, name: $name, layers: ${layers.toList}}""", s => logger.info(s)) {
        val diff = rdd2 localSubtract rdd1
        val summary = diff.zonalMean(polygon)

        logger.info(s"LOCAL SUBTRACT ZONAL MEAN $summary")
      }
    }

    // Run benchmarks that do not filter
    val name = "unbounded"
    def rdd1 = getRdd(catalog, layers(0), name)
    def rdd2 = getRdd(catalog, layers(1), name)

    Timer.timedTask(s"""Benchmark: {type: SingleModel-MinMax, name: $name, layers: ${layers.toList}}""", s=> logger.info(s)) {
      val (min, max) = rdd1.minMax

      logger.info(s"SINGLE MODEL MIN MAX: ${(min, max)}")
    }

    Timer.timedTask(s"""Benchmark: {type: SingleModel-Average, name: $name, layers: ${layers.toList}}""", s=> logger.info(s)) {
      val avg = annualAverage(rdd1)

      logger.info(s"SINGLE MODEL YEARLY AVERAGE OF DIFFERENCES ${avg}")
    }

    Timer.timedTask(s"""Benchmark: {type: MultiModel-localSubtract-Average, name: $name, layers: ${layers.toList}}""", s=> logger.info(s)) {
      val diff = rdd2 localSubtract rdd1
      val avg = annualAverage(diff)

      logger.info(s"MULTI MODEL YEARLY AVERAGE $avg")
    }

    Timer.timedTask(s"""Benchmark: {type: MultiModel-localMean-MinMax, name: $name, layers: ${layers.toList}}""", s=> logger.info(s)) {
      val diff = Seq(rdd1, rdd2) localMean
      val minMax = diff.minMax

      logger.info(s"MULTI MODEL MIN MAX ${minMax}")
    }

    Timer.timedTask(s"""Benchmark: {type: MultiModel-localSubtract-count, name: $name, layers: ${layers.toList}}""", s => logger.info(s)) {
      val diff = rdd2 localSubtract rdd1
      val count = diff.count

      logger.info(s"LOCAL SUBTRACT RECORD COUNT: ${count}")
    }

    Timer.timedTask(s"""Benchmark: {type: MultiModel-localSubtract-save, name: $name, layers: ${layers.toList}}""", s => logger.info(s)) {
      val diff = rdd2 localSubtract rdd1

      val layerId = LayerId("benchmark-output", 8)
      catalog.writer[SpaceTimeKey](ZCurveKeyIndexMethod.byYear, "benchmarkoutput").write(layerId, diff)

      logger.info(s"LOCAL SUBTRACT WRITE TO CATALOG: done.")
    }
  }
}

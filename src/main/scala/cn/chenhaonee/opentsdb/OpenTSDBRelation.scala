package cn.chenhaonee.opentsdb

import cn.chenhaonee.OpenTSDB4J.impl.OpenTSDBClientImpl
import cn.chenhaonee.OpenTSDB4J.vo.Query
import com.google.common.collect.Lists
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, RowFactory, SQLContext}

import scala.collection.JavaConversions._


private[sql] class OpenTSDBRelation(
                                     val inputRDD: Option[RDD[String]],
                                     val start: String,
                                     val end: String,
                                     val metrics: String,
                                     val tagK: String,
                                     val tagV: String,
                                     val url: String,
                                     val maybeDataSchema: Option[StructType]
                                   )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  override def buildScan(): RDD[Row] = {
    val metric = metrics.split(",")(0)
    val subQuery = new Query.SubQuery("sum", metric, tagK, tagV)
    val subQueries = Lists.newArrayList(subQuery)
    val query = new Query(start, end, subQueries)


    val opentsdb = OpenTSDBClientImpl.build(url + "/api/query?details")
    val result = opentsdb.queryFromDB(query)
    val rows = result.toList.map(dp => {
      RowFactory.create(dp.getTimestamp, dp.getMetric, dp.getValue)
    })
    sqlContext.sparkContext.parallelize(rows.toSeq)
  }

  override def schema: StructType = StructType(
    Array(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("metric", StringType, nullable = false),
      StructField("value", DoubleType, nullable = false),
      //StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false),
    )
  )

  class TSData(
                val timestamp: TimestampType,
                val metric: String,
                val value: Double,
                val tags: Map[String, String]
              )
}
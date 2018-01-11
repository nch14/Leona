package cn.skydata.opentsdb

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  //with CreatableRelationProvider
  with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val url = parameters.getOrElse("url", "")
    val start = parameters.getOrElse("start", "")
    val end = parameters.getOrElse("end", "")
    val metric = parameters.getOrElse("metric", "")
    val tagK = parameters.getOrElse("tagK", "")
    val tagV = parameters.getOrElse("tagV", "")

    new OpenTSDBRelation(None, start, end, metric, tagK, tagV, url, None)(sqlContext)

    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String], schema: StructType): BaseRelation = {

    val url = parameters.getOrElse("url", "")
    val start = parameters.getOrElse("start", "")
    val end = parameters.getOrElse("end", "")
    val metric = parameters.getOrElse("metric", "")
    val tagK = parameters.getOrElse("tagK", "")
    val tagV = parameters.getOrElse("tagV", "")

    new OpenTSDBRelation(None, start, end, metric, tagK, tagV, url, Option.apply(schema))(sqlContext)
  }

  override def shortName(): String = "OpenTSDB"
}


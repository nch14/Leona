package cn.chenhaonee

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark
      .read
      .format("com.nico.datasource.dat")
      .load("/Users/anicolaspp/data/")

    df.write
      .format("com.nico.datasource.dat")
      .save("/Users/anicolaspp/data/output")
  }
}

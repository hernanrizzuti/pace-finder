package com.rizzutih.pacefinder.sparkConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionConfig {

  protected def sparkConfig: SparkConf

  def sparkSession(): SparkSession = {
    lazy val spark: SparkSession = SparkSession.builder()
      .appName("pace-finder")
      .config(sparkConfig)
      .getOrCreate()
    spark
  }
}

class LocalSparkSessionConfig extends SparkSessionConfig {
  override protected def sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").set("spark.driver.host", "localhost")

  override def sparkSession(): SparkSession = {
    val spark = super.sparkSession()
    spark
  }
}

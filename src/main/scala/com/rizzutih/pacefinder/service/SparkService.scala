package com.rizzutih.pacefinder.service

import com.rizzutih.pacefinder.sparkConfig.SparkSessionConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class SparkService(val sparkSessionConfig: SparkSessionConfig) {

  def spark: SparkSession = {
    sparkSessionConfig.sparkSession()
  }

  def loadParquetData(path: String): DataFrame = {
    spark.read.parquet(path)
  }

  def loadCsvData(path: String): DataFrame = {
    spark.read
      .options(Map("header" -> "true", "delimiter" -> ",", "inferSchema" -> "true"))
      .csv(path)
  }

  def writeData(dataFrame: DataFrame,
                path: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).json(path)
  }

  def executeSql(sql: String): DataFrame = {
    spark.sql(sql)
  }

  def stopSession(): Unit = {
    spark.stop()
  }

}

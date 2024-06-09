package com.rizzutih.pacefinder.service

import com.rizzutih.pacefinder.sparkConfig.SparkSessionConfig
import com.rizzutih.test.UnitSpec
import org.apache.spark.sql._

class SparkServiceTest extends UnitSpec {

  private val sparkSessionConfig: SparkSessionConfig = mock[SparkSessionConfig]
  private val spark: SparkSession = mock[SparkSession]
  private val dataFrameReader: DataFrameReader = mock[DataFrameReader]
  private val dataFrameWriter: DataFrameWriter[Row] = mock[DataFrameWriter[Row]]
  private val dataFrame: DataFrame = mock[DataFrame]

  before {
    sparkSessionConfig.sparkSession() shouldReturn spark
  }

  "spark" should "call sparkSession" in {
    //given
    val sparkService = new SparkService(sparkSessionConfig)
    //when
    sparkService.spark
    //then
    sparkSessionConfig.sparkSession() wasCalled once
  }

  "loadParquetData" should "read parquet" in {
    //given
    val sparkService = new SparkService(sparkSessionConfig)
    val path = "some/dir/"
    spark.read shouldReturn dataFrameReader
    //when
    sparkService.loadParquetData(path)
    //then
    dataFrameReader.parquet(path) wasCalled once
  }

  "loadCsvData" should "read csv" in {
    //given
    val sparkService = new SparkService(sparkSessionConfig)
    val path = "some/dir/"
    spark.read shouldReturn dataFrameReader
    dataFrameReader.options(Map("header" -> "true", "delimiter" -> ",", "inferSchema" -> "true")) shouldReturn dataFrameReader
    //when
    sparkService.loadCsvData(path)
    //then
    dataFrameReader.csv(path) wasCalled once
  }

  "writeData" should "write parquet" in {
    //given
    val sparkService = new SparkService(sparkSessionConfig)
    val path = "some/dir/"
    dataFrame.write shouldReturn dataFrameWriter
    dataFrameWriter.mode(SaveMode.Overwrite) shouldReturn dataFrameWriter
    //when
    sparkService.writeData(dataFrame, path)
    //then
    dataFrameWriter.json(path) wasCalled once
  }

  "executeSql" should "call spark sql" in {
    //given
    val sparkService = new SparkService(sparkSessionConfig)
    val sql = "select * from table"
    //when
    sparkService.executeSql(sql)
    //then
    spark.sql(sql) wasCalled once
  }

  "stopSession" should "call spark stop" in {
    //given
    val sparkService = new SparkService(sparkSessionConfig)
    //when
    sparkService.stopSession()
    //then
    spark.stop() wasCalled once
  }

}

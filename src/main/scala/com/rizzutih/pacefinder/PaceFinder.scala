package com.rizzutih.pacefinder

import com.rizzutih.pacefinder.service.{MetricDefinitionLoader, SparkService}
import com.rizzutih.pacefinder.sparkConfig.LocalSparkSessionConfig

object PaceFinder extends App {

  val sparkService = new SparkService(new LocalSparkSessionConfig())
  val baseDir = ""

  val metricDefinitionsDataset = sparkService.loadCsvData("src/main/resources/metric_definitions.csv")
  val metricDefinitions = new MetricDefinitionLoader().getAllDefinitions(metricDefinitionsDataset)
  val activities = sparkService.loadParquetData(s"$baseDir/activities.parquet")

  activities.createOrReplaceTempView("activity")

  metricDefinitions.foreach(m => {
    val result = sparkService.executeSql(m.sqlScript)
    sparkService.writeData(result, s"$baseDir/${m.destinationDir}/")
    result.show(false)
  })

  sparkService.stopSession()

}

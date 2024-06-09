package com.rizzutih.pacefinder

import com.rizzutih.pacefinder.service.{MetricDefinitionLoader, SparkService}
import com.rizzutih.pacefinder.sparkConfig.LocalSparkSessionConfig

object PaceFinder extends App {
//TODO: create more metrics, extract logic from here and create a service, create an argumentProcessor, update readme.
  val sparkService = new SparkService(new LocalSparkSessionConfig())
  val baseDir = ""

  val metricDefinitionsDataset = sparkService.loadCsvData("src/main/resources/metric_definitions.csv")
  val metricDefinitions = new MetricDefinitionLoader().getAllDefinitions(metricDefinitionsDataset)
  val activities = sparkService.loadParquetData(s"$baseDir/activities-5-years.parquet")

  activities.createOrReplaceTempView("activity")

  metricDefinitions.foreach(m => {
    val result = sparkService.executeSql(m.sqlScript)
    sparkService.writeData(result, s"$baseDir/${m.destinationDir}/")
//    result.show(1000, false)
  })

  sparkService.stopSession()

}

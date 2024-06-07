package com.rizzutih.pacefinder.service

import org.apache.spark.sql.DataFrame

class MetricDefinitionLoader {

  def getAllDefinitions(metricDefinitionDataset: DataFrame): List[MetricDefinition] = {
    metricDefinitionDataset.collect()
      .map(m => MetricDefinition(m.getString(0), m.getString(1), m.getString(2)))
      .toList
  }

  sealed case class MetricDefinition(description: String,
                                     destinationDir: String,
                                     sqlScript: String)


}

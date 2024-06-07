package com.rizzutih.pacefinder.service

import com.rizzutih.pacefinder.sparkConfig.LocalSparkSessionConfig
import com.rizzutih.test.UnitSpec

class MetricDefinitionLoaderTest extends UnitSpec {

  "getAllDefinitions" should "retrieve all metric definitions" in {
    //given
    val spark = new LocalSparkSessionConfig().sparkSession()
    import spark.implicits._
    val metricDefOne = ("metric 1 description ", "location where metric 1 will be written", "select * from table")
    val metricDefTwo = ("metric 2 description ", "location where metric 2 will be written", "select count(*) from table")
    val metricDefDataset = Seq(metricDefOne, metricDefTwo).toDF("description", "destination_dir", "sql")
    //when
    val metricDefinitions = new MetricDefinitionLoader().getAllDefinitions(metricDefDataset)

    //then
    metricDefinitions.size shouldBe 2
    metricDefinitions(0).description shouldBe metricDefOne._1
    metricDefinitions(0).destinationDir shouldBe metricDefOne._2
    metricDefinitions(0).sqlScript shouldBe metricDefOne._3
    metricDefinitions(1).description shouldBe metricDefTwo._1
    metricDefinitions(1).destinationDir shouldBe metricDefTwo._2
    metricDefinitions(1).sqlScript shouldBe metricDefTwo._3

  }

}

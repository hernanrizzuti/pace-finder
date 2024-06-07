package com.rizzutih.pacefinder.sparkConfig

import com.rizzutih.test.UnitSpec

class SparkSessionConfigTest extends UnitSpec {


  "LocalSparkSessionConfig" should "be configured" in {
    //when
    val spark = new LocalSparkSessionConfig().sparkSession()
    val conf = spark.conf
    //then
    conf.get("spark.master") shouldBe "local[*]"
    conf.get("spark.driver.host") shouldBe "localhost"
    conf.get("spark.app.name") shouldBe "pace-finder"
  }

}

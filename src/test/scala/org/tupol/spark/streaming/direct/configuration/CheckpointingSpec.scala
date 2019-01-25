package org.tupol.spark.streaming.direct.configuration

import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import org.scalatest.{ FunSuite, Matchers }

class CheckpointingSpec extends FunSuite with Matchers {

  test("from no configuration should be None") {
    val config = ConfigFactory.empty
      .withValue("incorrect.checkpointing.directory", ConfigValueFactory.fromAnyRef("dd"))

    Checkpointing.validationNel(config).toOption shouldBe None
  }

  test("from correct configuration should work") {
    val config = ConfigFactory.empty
      .withValue("checkpointing.directory", ConfigValueFactory.fromAnyRef("dd"))

    Checkpointing.validationNel(config).toOption shouldBe Some(Checkpointing("dd"))

  }

}

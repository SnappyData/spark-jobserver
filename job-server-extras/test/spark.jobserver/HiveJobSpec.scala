/*
 * Changes for TIBCO Project SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.context.{HiveContextLike, SparkContextFactory}
import spark.jobserver.io.{JobDAO, JobDAOActor}

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}

class TestHiveContextFactory extends SparkContextFactory {

  type C = TestHiveContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    contextFactory(sparkConf)
  }

  protected def contextFactory(conf: SparkConf): C =
    new TestHiveContext(new SparkContext(conf)) with HiveContextLike
}

object HiveJobSpec extends JobSpecConfig {
  override val contextFactory = classOf[TestHiveContextFactory].getName
}

class HiveJobSpec extends ExtrasJobSpecBase(HiveJobSpec.getNewSystem) {
  import scala.concurrent.duration._

  import CommonMessages._

  val classPrefix = "spark.jobserver."
  private val hiveLoaderClass = classPrefix + "HiveLoaderJob"
  private val hiveQueryClass = classPrefix + "HiveTestJob"

  val emptyConfig = ConfigFactory.parseString("spark.master = bar")
  val queryConfig = ConfigFactory.parseString(
                      """sql = "SELECT firstName, lastName FROM `default`.`test_addresses` WHERE city = 'San Jose'" """)

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props(
                             HiveJobSpec.getContextConfig(false, HiveJobSpec.contextConfig)))
  }

  describe("Spark Hive Jobs") {
    it("should be able to create a Hive table, then query it using separate Hive-SQL jobs") {
      manager ! JobManagerActor.Initialize(daoActor, None)
      expectMsgClass(30 seconds, classOf[JobManagerActor.Initialized])

      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", hiveLoaderClass, emptyConfig, syncEvents ++ errorEvents)
      expectMsgPF(120 seconds, "Did not get JobResult") {
        case JobResult(_, result: Long) => result should equal (3L)
      }
      expectNoMsg()

      manager ! JobManagerActor.StartJob("demo", hiveQueryClass, queryConfig, syncEvents ++ errorEvents)
      expectMsgPF(6 seconds, "Did not get JobResult") {
        case JobResult(_, result: Array[Row]) =>
          result should have length (2)
          result(0)(0) should equal ("Bob")
      }
      expectNoMsg()
    }
  }
}

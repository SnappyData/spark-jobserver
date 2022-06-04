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
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext

/**
 * A test job that accepts a HiveContext, as opposed to the regular SparkContext.
 * Initializes some dummy data into a table, reads it back out, and returns a count
 * (Will create Hive metastore at job-server/metastore_db if Hive isn't configured)
 */
object HiveLoaderJob extends SparkHiveJob {
  // The following data is stored at ./hive_test_job_addresses.txt
  // val addresses = Seq(
  //   Address("Bob", "Charles", "101 A St.", "San Jose"),
  //   Address("Sandy", "Charles", "10200 Ranch Rd.", "Purple City"),
  //   Address("Randy", "Charles", "101 A St.", "San Jose")
  // )

  val tableCreate = "CREATE TABLE `default`.`test_addresses`"
  val tableArgs = "(`firstName` String, `lastName` String, `address` String, `city` String)"
  val tableRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'"
  val tableColFormat = "COLLECTION ITEMS TERMINATED BY '\u0002'"
  val tableMapFormat = "MAP KEYS TERMINATED BY '\u0003' STORED"
  val tableAs = "AS TextFile"

  // Will fail with a 'SemanticException : Invalid path' if this file is not there
  val loadPath = "'test/spark.jobserver/hive_test_job_addresses.txt'"

  def validate(hive: HiveContext, config: Config): SparkJobValidation = SparkJobValid

  def runJob(hive: HiveContext, config: Config): Any = {
    hive.sql("DROP TABLE if exists `default`.`test_addresses`")
    hive.sql(s"$tableCreate $tableArgs $tableRowFormat $tableColFormat $tableMapFormat $tableAs")

    hive.sql(s"LOAD DATA LOCAL INPATH $loadPath OVERWRITE INTO TABLE `default`.`test_addresses`")
    val addrRdd = hive.sql("SELECT * FROM `default`.`test_addresses`")
    addrRdd.count()
  }
}

/**
 * This job simply runs the Hive SQL in the config.
 */
object HiveTestJob extends SparkHiveJob {
  def validate(hive: HiveContext, config: Config): SparkJobValidation = SparkJobValid

  def runJob(hive: HiveContext, config: Config): Any = {
    hive.sql(config.getString("sql")).collect()
  }
}

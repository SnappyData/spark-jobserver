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

import java.net.URL

import akka.actor.ActorRef
import akka.util.Timeout
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.JobDAOActor
import spark.jobserver.util.{ContextURLClassLoader, JarUtils, LRUCache}

import org.apache.spark.SparkContext

case class JobJarInfo(constructor: () => SparkJobBase,
                      className: String,
                      jarFilePath: String)

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the jar and load it every single time?
 */

class JobCache(maxEntries: Int, dao: ActorRef, sparkContext: SparkContext, loader: ContextURLClassLoader) {
  import scala.concurrent.duration._

  private val cache = new LRUCache[(String, DateTime, String), JobJarInfo](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(3 seconds)

  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the jar was uploaded
   * @param uploadTime the upload time for the version of the jar wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): (JobJarInfo , Boolean ) = {
    var retrievedFromCache = true
    ( cache.get((appName, uploadTime, classPath), {
      import scala.concurrent.Await

      import akka.pattern.ask
      retrievedFromCache=false
      val jarPathReq = (dao ? JobDAOActor.GetJarPath(appName, uploadTime)).mapTo[JobDAOActor.JarPath]
      val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).jarPath
      val jarFilePath = new java.io.File(jarPath).getAbsolutePath()
      // sparkContext.addJar(jarFilePath) // Adds jar for remote executors
      loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
      val constructor = JarUtils.loadClassOrObject[SparkJobBase](classPath, loader)
      JobJarInfo(constructor, classPath, jarFilePath)
    }), retrievedFromCache )
  }
}

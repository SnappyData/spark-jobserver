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

import akka.actor.{ActorSystem, ActorRef}
import akka.actor.Props
import akka.pattern.ask
import com.typesafe.config.{ConfigValueFactory, Config, ConfigFactory}

import java.io.File
import spark.jobserver.io.{JobDAOActor, JobDAO, DataFileDAO}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/**
 * The Spark Job Server is a web service that allows users to submit and run Spark jobs, check status,
 * and view results.
 * It may offer other goodies in the future.
 * It only takes in one optional command line arg, a config file to override the default (and you can still
 * use -Dsetting=value to override)
 * -- Configuration --
 * {{{
 *   spark {
 *     master = "local"
 *     jobserver {
 *       port = 8090
 *     }
 *   }
 * }}}
 */
object JobServer {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.

 /* SnappyData notes why we need makeConfig.

   Settings for jobserver overriding the application.conf or bootProperties.
   ideally there should have been reference.conf in the jobserver packaging instead of application.conf.

   in generic case, user supplied application.conf will be picked up if found first in the classpath.
   in snappy embeded mode properties file will be parsed much earlier & converted into bootProperties.
   user won't need application.conf in snappydata's case.

   to provide snappy defaults "reference.conf" should be used if we too move to typesafe.Config
   otherwise a separate file can be created "jobserver-reference.conf" which will be injected
   just before defaultConfig.

   ref:
   http://kamon.io/introduction/configuration/
   https://github.com/typesafehub/config
 */

  def start(args: Array[String], makeConfig: (Array[String]) => Config, makeSystem: Config => ActorSystem) {
    val config = makeConfig(args)

    logger.debug("Starting JobServer with config {}", config.getConfig("spark").root.render())
    logger.debug("Spray config: {}", config.getConfig("spray.can.server").root.render())
    val port = config.getInt("spark.jobserver.port")

    // TODO: Hardcode for now to get going. Make it configurable later.
    val system = makeSystem(config)
    val clazz = Class.forName(config.getString("spark.jobserver.jobdao"))
    val ctor = clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    try {
      val jobDAO = ctor.newInstance(config).asInstanceOf[JobDAO]
      val daoActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO), "dao-manager")
      val dataManager = system.actorOf(Props(classOf[DataManagerActor],
          new DataFileDAO(config)), "data-manager")
      val jarManager = system.actorOf(Props(classOf[JarManager], daoActor), "jar-manager")
      val contextPerJvm = config.getBoolean("spark.jobserver.context-per-jvm")
      val supervisor =
        system.actorOf(Props(if (contextPerJvm) { classOf[AkkaClusterSupervisorActor] }
                             else               { classOf[LocalContextSupervisorActor] }, daoActor),
                       "context-supervisor")
      val jobInfo = system.actorOf(Props(classOf[JobInfoActor], jobDAO, supervisor), "job-info")

      // Add initial job JARs, if specified in configuration.
      storeInitialJars(config, jarManager)

      // Create initial contexts
      supervisor ! ContextSupervisor.AddContextsFromConfig
      new WebApi(system, config, port, jarManager, dataManager, supervisor, jobInfo).start()
    } catch {
      case e: Exception =>
        logger.error("Unable to start Spark JobServer: ", e)
        sys.exit(1)
    }

  }

  private def storeInitialJars(config: Config, jarManager: ActorRef): Unit = {
    val initialJarPathsKey = "spark.jobserver.job-jar-paths"
    if (config.hasPath(initialJarPathsKey)) {
      val initialJarsConfig = config.getConfig(initialJarPathsKey).root

      logger.info("Adding initial job jars: {}", initialJarsConfig.render())

      val initialJars =
        initialJarsConfig
          .asScala
          .map { case (key, value) => (key, value.unwrapped.toString) }
          .toMap

      // Ensure that the jars exist
      for(jarPath <- initialJars.values) {
        val f = new java.io.File(jarPath)
        if (!f.exists) {
          val msg =
            if (f.isAbsolute) {
              s"Initial Jar File $jarPath does not exist"
            } else {
              s"Initial Jar File $jarPath (${f.getAbsolutePath}) does not exist"
            }

          throw new java.io.IOException(msg)
        }
      }

      val contextTimeout = util.SparkJobUtils.getContextTimeout(config)
      val future =
        (jarManager ? StoreLocalJars(initialJars))(contextTimeout.seconds)

      Await.result(future, contextTimeout.seconds) match {
        case InvalidJar => sys.error("Could not store initial job jars.")
        case _ =>
      }
    }
  }

  /**
   * This will allow a custom Config object to be built by the user from disparate sources.
   *
   * @param args - arguments passed to start or commandline.
   * @return a Config object that will only be used "as is". User's responsibility to call
   *         ConfigFactory.load and use it as .withFallback.
   */
  private def getConfig(args: Array[String]) : Config = {
    val defaultConfig = ConfigFactory.load()
    if (args.length > 0) {
      val configFile = new File(args(0))
      if (!configFile.exists()) {
        println("Could not find configuration file " + configFile)
        sys.exit(1)
      }
      ConfigFactory.parseFile(configFile).withFallback(defaultConfig).resolve()
    } else {
      defaultConfig
    }
  }

  def main(args: Array[String]) {
    import scala.collection.JavaConverters._
    def makeSupervisorSystem(name: String)(config: Config): ActorSystem = {
      val configWithRole = config.withValue("akka.cluster.roles",
        ConfigValueFactory.fromIterable(List("supervisor").asJava))
      ActorSystem(name, configWithRole)
    }
    start(args, getConfig, makeSupervisorSystem("JobServer")(_))
  }


}

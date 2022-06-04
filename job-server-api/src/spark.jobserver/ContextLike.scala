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

import spark.jobserver.util.ContextURLClassLoader

import org.apache.spark.SparkContext

/**
 * Represents a context based on SparkContext.  Examples include:
 * StreamingContext, SQLContext.
 *
 * The Job Server can spin up not just a vanilla SparkContext, but anything that
 * implements ContextLike.
 */
trait ContextLike {
  /**
   * The underlying SparkContext
   */
  def sparkContext: SparkContext

  /**
   * Returns true if the job is valid for this context.
   * At the minimum this should check for if the job can actually take a context of this type;
   * for example, a SQLContext should only accept jobs that take a SQLContext.
   * The recommendation is to define a trait for each type of context job;  the standard
   * [[DefaultSparkContextFactory]] checks to see if the job is of type [[SparkJob]].
   */
  def isValidJob(job: SparkJobBase): Boolean

  /**
   * Responsible for performing any cleanup, including calling the underlying context's
   * stop method.
   */
  def stop()


  def makeClassLoader(parent : ContextURLClassLoader): ContextURLClassLoader ={
    new ContextURLClassLoader(parent.getURLs, parent)
  }

  def addJobJar(jarName : String) = {
    // do nothing for not required cases like HiveContextFactory
  }
}
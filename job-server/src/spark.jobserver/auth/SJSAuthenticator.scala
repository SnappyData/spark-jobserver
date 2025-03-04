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
package spark.jobserver.auth

import java.sql.DriverManager

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import spray.routing.directives.AuthMagnet
import spray.routing.authentication.UserPass
import spray.routing.authentication.BasicAuth
import spray.routing.authentication._
import spray.routing.directives.AuthMagnet

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc._
import org.apache.shiro.authz.AuthorizationException
import org.apache.shiro.util.Factory
import org.apache.shiro.subject.Subject

/**
 * Apache Shiro based authenticator for the Spark JobServer, the authenticator realm must be
 * specified in the ini file for Shiro
 */
trait SJSAuthenticator {

  import scala.concurrent.duration._

  def asShiroAuthenticator(authTimeout : Int)(implicit ec: ExecutionContext): AuthMagnet[AuthInfo] = {
    val logger = LoggerFactory.getLogger(getClass)

    def validate(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
      //if (!currentUser.isAuthenticated()) {
      Future {
        explicitValidation(userPass getOrElse UserPass("",""), logger)
      }
    }

    def authenticator(userPass: Option[UserPass]): Future[Option[AuthInfo]] = Future {
      Await.result(validate(userPass), authTimeout.seconds)
    }

    BasicAuth(authenticator _, realm = "Shiro Private")
  }

 /**
   * do not call directly - only for unit testing!
   */
  def explicitValidation(userPass: UserPass, logger: Logger): Option[AuthInfo] = {
    import collection.JavaConverters._
    val currentUser = SecurityUtils.getSubject()
    val UserPass(user, pass) = userPass
    val token = new UsernamePasswordToken(user, pass)
    try {
      currentUser.login(token)
      val fullName = currentUser.getPrincipal().toString
      //is this user allowed to do anything -
      //  realm implementation may for example throw an exception
      //  if user is not a member of a valid group
      currentUser.isPermitted("*")
      logger.trace("ACCESS GRANTED, user [%s]", fullName)
      currentUser.logout()
      Option(new AuthInfo(new User(fullName)))
    } catch {
      case uae: UnknownAccountException =>
        logger.info("ACCESS DENIED (Unknown), user [" + user + "]")
        None
      case ice: IncorrectCredentialsException =>
        logger.info("ACCESS DENIED (Incorrect credentials), user [" + user + "]")
        None
      case lae: LockedAccountException =>
        logger.info("ACCESS DENIED (Account is locked), user [" + user + "]")
        None
      case ae: AuthorizationException =>
        logger.info("ACCESS DENIED (" + ae.getMessage() + "), user [" + user + "]")
        None
      case ae: AuthenticationException =>
        logger.info("ACCESS DENIED (Authentication Exception), user [" + user + "]")
        None
    }
  }

  /**
   * default authenticator that accepts all users
   * based on example provided by Mario Camou
   * at
   * http://www.tecnoguru.com/blog/2014/07/07/implementing-http-basic-authentication-with-spray/
   */
  def asAllUserAuthenticator(implicit ec: ExecutionContext): AuthMagnet[AuthInfo] = {
    def validateUser(userPass: Option[UserPass]): Option[AuthInfo] = {

      Some(new AuthInfo(new User("anonymous")))
    }

    def authenticator(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
      Future { validateUser(userPass) }
    }

    BasicAuth(authenticator _, realm = "Private API")
  }

  def snappyUserAuthenticator(implicit ec: ExecutionContext): AuthMagnet[AuthInfo] = {
    BasicAuth(SnappyAuthenticator.auth.authenticate _, realm = "Private API")
  }
}

trait SnappyAuthenticator {
  def authenticate(userPass: Option[UserPass]): Future[Option[AuthInfo]]
}

object SnappyAuthenticator {
  import scala.concurrent.ExecutionContext.Implicits.global
  var auth: SnappyAuthenticator = new SnappyAuthenticator {
    override def authenticate(userPass: Option[UserPass]): Future[Option[AuthInfo]] = {
      Future { Some(new AuthInfo(User("anonymous"))) }
    }
  }
}

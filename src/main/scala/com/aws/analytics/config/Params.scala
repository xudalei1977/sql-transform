package com.aws.analytics.config

/**
  * Project: mysql-redshift-loader
  * Author: shivamsharma
  * Date: 12/29/16.
  */
private[analytics] sealed trait Param

private[analytics] object Params {

    case class AppParams(tableDetailsPath: String,
                         mailDetailsPath: String,
                         alertOnFailure: Boolean = false,
                         retryCount: Int = 0,
                         logMetricsReporting: Boolean = false,
                         jmxMetricsReporting: Boolean = false,
                         metricsWindowSize: Long = 5) extends Param

    case class MailParams(host: String,
                          port: Int = 25,
                          username: String,
                          password: Option[String] = None,
                          to: String,
                          cc: String,
                          subject: String) extends Param

}
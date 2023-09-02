package com.aws.analytics.util

import com.aws.analytics.config.DBConfig
import com.aws.analytics.config.InternalConfs.{InternalConfig, TableDetails}

import java.sql.Connection
import scala.collection.immutable.Seq


private[analytics] trait DBEngineUtil{
  def queryByJDBC(conf: DBConfig, sql: String) : Seq[String]
  def getConnection(conf: DBConfig): Connection
  def getValidFieldNames(mysqlConfig: DBConfig, internalConfig: InternalConfig)(implicit crashOnInvalidType: Boolean): TableDetails
  def transferDateFunction(sql:String): String
}

package com.aws.analytics

import org.slf4j.{Logger, LoggerFactory}
import com.aws.analytics.config.DBConfig
import com.aws.analytics.util._
import com.aws.analytics.config.InternalConfs
import com.aws.analytics.config.InternalConfs.InternalConfig

import java.io._

object ExternalTable {

  private val logger: Logger = LoggerFactory.getLogger(ExternalTable.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("start the create external table and insert data into ...")
    val params = DBConfig.parseConfig(ExternalTable, args)
    println(s"params := ${params.toString}")

    // get all tables in the database to migrate.
    val dbEngine:DBEngineUtil = params.dbEngine match {
      case "adb-mysql" =>
        new ADBMySQLUtil()
      case "adb-pg" =>
        new ADBPostgreSQLUtil()
      case _ =>
        new ADBMySQLUtil()
    }

    val sql:String = params.dbEngine match {
      case "adb-mysql" => "show tables"
      case "adb-pg" => s"select tablename from pg_tables where schemaname = '${params.schema}'"
      case _ => "show tables"
    }

    val allTable = dbEngine.queryByJDBC(params, sql)
    allTable.foreach(tableName => {

      val conf = DBConfig(hostname = params.hostname,
                          portNo = params.portNo,
                          userName = params.userName,
                          password = params.password,
                          database = params.database,
                          schema = params.schema,
                          tableName = tableName,
                          ossEndpoint = params.ossEndpoint,
                          ossUrl = params.ossUrl,
                          accessID = params.accessID,
                          accessKey = params.accessKey,
                          ossFormat = params.ossFormat,
                          ossFilter = params.ossFilter)

      dbEngine.createAndInsertExternalTable(conf)
    })
  }
}

//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.ExternalTable \
//  -g adb-mysql -h emr-workshop-mysql8.chl9yxs6uftz.us-east-1.rds.amazonaws.com \
//  -p 3306 -d dev -u admin -w Password**** \
//  -e oss-cn-hangzhou-internal.aliyuncs.com \
//  -u oss://<bucket-name>/adb_data/ \
//  -i LTA*********
//  -k Ccw*********
//  -f parquet

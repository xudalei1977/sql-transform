package com.aws.analytics

import org.slf4j.{Logger, LoggerFactory}
import com.aws.analytics.config.DBConfig
import com.aws.analytics.util._
import com.aws.analytics.config.InternalConfs
import com.aws.analytics.config.InternalConfs.InternalConfig

import java.io._

object CommonSQL {

  private val logger: Logger = LoggerFactory.getLogger(CommonSQL.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("start the sql script transform ...")
    val params = DBConfig.parseConfig(CommonSQL, args)
    println(s"params := ${params.toString}")

    val dbEngine:DBEngineUtil = params.dbEngine match {
      case "adb-mysql" =>
        new ADBMySQLUtil()
      case "adb-pg" =>
        new ADBPostgreSQLUtil()
      case _ =>
        new ADBMySQLUtil()
    }

    new java.io.File(params.directory).listFiles.filter(_.getName.endsWith(".sql")).foreach( file => {
      val source = scala.io.Source.fromFile(params.directory + "/" + file.getName)
      val sql = try source.mkString finally source.close()

      println("======== old sql:")
      println(sql)
      var newSql = dbEngine.transferDateFunction(sql)
      println("======== new sql:")
      println(newSql)

      val newFile = new File(params.directory + "/new_" + file.getName)
      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newFile), "UTF-8"))
      bw.write(newSql)
      bw.close()
    })

  }

}

//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.CommonSQL -g adb-pg -r /home/ec2-user/tpch_query

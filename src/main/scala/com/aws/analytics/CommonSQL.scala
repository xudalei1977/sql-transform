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

    logger.info("start the ETLJobSQL script transform ...")
    val params = DBConfig.parseConfig(CreateTableSQL, args)
    println(s"params := ${params.toString}")

    val dbEngine:DBEngineUtil = params.dbEngine match {
      case "mysql" =>
        new MySQLUtil()
      case "pg" =>
        new PostgreSQLUtil()
      case _ =>
        new MySQLUtil()
    }

    new java.io.File(params.directory).listFiles.filter(_.getName.endsWith(".sql")).foreach( file => {
      val source = scala.io.Source.fromFile(file.getName)
      val sql = try source.mkString finally source.close()

      var newSql = dbEngine.transferDateFunction(sql)

      val newFile = new File("new_" + file.getName)
      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newFile), "UTF-8"))
      bw.write(newSql)
      bw.close()
    })

  }

}


//scala com.aws.analytics.CommonSQL \
//  -g adb_pg \
//  -r /home/ec2-user/adb_sql

package com.aws.analytics

import org.slf4j.{Logger, LoggerFactory}
import com.aws.analytics.config.DBConfig
import com.aws.analytics.util._
import com.aws.analytics.config.InternalConfs
import com.aws.analytics.config.InternalConfs.InternalConfig

import java.io._

object CreateTableSQL {

  private val logger: Logger = LoggerFactory.getLogger(CreateTableSQL.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("start the create table sql script transform ...")
    val params = DBConfig.parseConfig(CreateTableSQL, args)
    println(s"params := ${params.toString}")
    val file = new File(params.fileName)
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))

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
    allTable.foreach( tableName => {

      val conf = DBConfig(hostname = params.hostname,
                          portNo = params.portNo,
                          userName = params.userName,
                          password = params.password,
                          database = params.database,
                          schema = params.schema,
                          tableName = tableName)

      val tableDetails = dbEngine.getValidFieldNames(conf, internalConfig = InternalConfig())(false)
      val createTableString = RedshiftUtil.getCreateTableString(tableDetails, conf)

      bw.write(s"---------- create table ${tableName} begin ----------\n")
      bw.write(createTableString.toLowerCase())
      bw.write("\n")
      bw.write(s"---------- create table ${tableName} end ----------\n")
      bw.write("\n")
      bw.write("\n")

    })

    bw.close()
  }

}

//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.CreateTableSQL \
//  -f /home/ec2-user/create_table_redshift.sql -g adb-mysql \
//  -h emr-workshop-mysql8.chl9yxs6uftz.us-east-1.rds.amazonaws.com \
//  -p 3306 -d dev -u admin -w Password****

//scala com.aws.analytics.CreateTableSQL \
//  -f /home/ec2-user/create_table_redshift.sql -g adb-pg \
//  -h gp-bp1t4m428azo2zxk0o-master.gpdb.rds.aliyuncs.com \
//  -p 5432 -d adb_sampledata_tpch -s public -u postgres -w Password****

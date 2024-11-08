package com.aws.analytics

import com.aws.analytics.config.DBConfig
import com.aws.analytics.config.InternalConfs.InternalConfig
import com.aws.analytics.util._
import org.slf4j.{Logger, LoggerFactory}
import java.io._


object MySQL2Hive {

  private val logger: Logger = LoggerFactory.getLogger(MySQL2Hive.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("start the create table sql script transform ...")
    val params = DBConfig.parseConfig(MySQL2Hive, args)
    println(s"params := ${params.toString}")

    val ddlDir = new File(params.ddlDir + "/" + params.database)
    if (! ddlDir.exists())
      ddlDir.mkdirs()

    val mySQLUtil = new MySQLUtil()
    val hiveUtil = new HiveUtil()

    val allTable: Seq[String] = if (params.tableName != "") Seq(params.tableName)
                                  else mySQLUtil.queryByJDBC(params, "show tables")
    /** create table sql */
    allTable.foreach(tableName => {
      val conf = DBConfig(hostname = params.hostname,
                          portNo = params.portNo,
                          userName = params.userName,
                          password = params.password,
                          database = params.database,
                          schema = params.schema,
                          tableName = tableName)

      val tableDetails = mySQLUtil.getTableDetails(conf, internalConfig = InternalConfig())(false)
      val (createTableString, partitionOpt) = hiveUtil.getCreateTableString(tableDetails, conf)
      if (partitionOpt.getOrElse("") != "") {
        val file = new File(ddlDir.toString + "/" + tableName + ".sql")
        write2File(createTableString, file)
      }
    })

    /** data migration use AWS DMS, ignore here. */

    /** sql script transform from mysql to hive
     *  currently, only provide the Date function transform, will add more by regex, and test GenAI to transform. */
    new java.io.File(params.sqlDir).listFiles.filter(_.getName.endsWith(".sql")).foreach( file => {
      val source = scala.io.Source.fromFile(params.sqlDir + "/" + file.getName)
      val sql = try source.mkString finally source.close()

      println("======== old sql:")
      println(sql)
      val newSql = mySQLUtil.transferDateFunction(sql)
      println("======== new sql:")
      println(newSql)

      val newFile = new File(params.sqlDir + "/new_" + file.getName)
      write2File(newSql, newFile)
    })
  }

  private def write2File(ddl: String, file: File): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))
    bw.write(ddl.toLowerCase())
    bw.write("\n")
    bw.close()
  }

}

// export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
// scala com.aws.analytics.CommonSQL -g adb-pg -r /home/ec2-user/tpch_query
//  -e prod -h emr-workshop-mysql8.chl9yxs6uftz.us-east-1.rds.amazonaws.com \
//  -d salesdb -t demo_partition -u admin -p ******* -s s3a://dalei-demo/hive -r "'2015-02-16','2015-04-13'" -n 20 \
//  -H ip-10-0-0-139.ec2.internal -D dev -P 2024-10-16 -S s3a://dalei-demo/hive

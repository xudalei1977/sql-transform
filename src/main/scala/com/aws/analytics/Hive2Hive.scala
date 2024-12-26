package com.aws.analytics

import org.slf4j.{Logger, LoggerFactory}
import com.aws.analytics.config.DBConfig
import com.aws.analytics.util.{HiveUtil, _}

import java.io._

object Hive2Hive {

  private val logger: Logger = LoggerFactory.getLogger(Hive2Hive.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("Migration Aliyun EMR Hive to AWS EMR Hive ...")
    val params = DBConfig.parseConfig(Hive2Hive, args)
    println(s"params := ${params.toString}")

    val ddlDir = new File(params.ddlDir + "/" + params.hiveDatabase)
    if (! ddlDir.exists())
      ddlDir.mkdirs()

    val hiveUtil = new HiveUtil()

    val allTable: Seq[String] = if (params.tableName != "") Seq(params.tableName)
                                else hiveUtil.queryByJDBC(params, s"show tables in ${params.hiveDatabase}")

    /** create table sql */
    allTable.foreach(tableName => {
      logger.info(s"the table is $tableName")
      val createTableString = hiveUtil.getCreateTableString(tableName, params)
      val file = new File(ddlDir.toString + "/" + tableName + ".sql")
      write2File(createTableString, file)
    })

  }

  private def write2File(ddl: String, file: File): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))

    bw.write(ddl)
    bw.write("\n")
    bw.close()
  }
}

//export ALI_CLOUD_ACCESS_KEY_ID=<Your AK>
//export ALI_CLOUD_ACCESS_KEY_SECRET=<Your SK>
//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.Hive2Hive \
//-f /home/ecs-user/20241212_ddl/ \
//-H 10.0.0.170 -D dev -o s3://dalei-demo/tmp -O hdfs://ip-10-0-0-170.ec2.internal:8020/user/hive/warehouse

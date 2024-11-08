package com.aws.analytics

import org.slf4j.{Logger, LoggerFactory}
import com.aws.analytics.config.DBConfig
import com.aws.analytics.util._
import java.io._

object MaxCompute2Hive {

  private val logger: Logger = LoggerFactory.getLogger(MaxCompute2Hive.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("export maxcompute data to oss ...")
    val params = DBConfig.parseConfig(MaxCompute2Hive, args)
    println(s"params := ${params.toString}")

    val ddlDir = new File(params.ddlDir + "/" + params.database)
    if (! ddlDir.exists())
      ddlDir.mkdirs()

    val externalTableDir = new File(params.externalTableDir + "/" + params.database)
    if (! externalTableDir.exists())
      externalTableDir.mkdirs()

    val maxcomputeUtil2 = new MaxcomputeUtil2(params.mcRegion, params.database)

    val allTable: Seq[String] = if (params.tableName != "") Seq(params.tableName)
                                else maxcomputeUtil2.queryByJDBC(params, "show tables")

    /** create table sql */
    allTable.foreach(tableName => {
      val (createTableString, partitionColumns, isExternal) = maxcomputeUtil2.getTableDDL(tableName, params.hiveInS3Path)
      val file = new File(ddlDir.toString + "/" + tableName + ".sql")
      write2File(createTableString, file)
    })

    /** create external table and insert data, now the data is in OSS. */
    allTable.foreach(tableName => {
      try{
        val (externalTableSqL, isExternal) = maxcomputeUtil2.createAndInsertExternalTable(tableName, params.ossUrl, params.ossFilter)
        val file = new File(externalTableDir.toString + "/" + tableName + ".sql")
        if(! isExternal)
          write2File(externalTableSqL, file)
      } catch {
        case e: Exception => println(s"********* Error: $tableName could not be generated DDL with" + e.printStackTrace)
      }
    })
  }

  private def write2File(ddl: String, file: File): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))

    bw.write(s"---------- set variable ----------\n")
    bw.write("set odps.sql.allow.fullscan=true;\n")
    bw.write("set odps.sql.hive.compatible=true;\n")
    bw.write("set odps.sql.unstructured.oss.commit.mode=true;\n")
    bw.write("set odps.sql.unstructured.file.pattern.black.list=.*_SUCCESS$,.*.hive_staging.*;\n\n")

    bw.write(ddl)
    bw.write("\n")
    bw.close()
  }
}

//export ALI_CLOUD_ACCESS_KEY_ID=<Your AK>
//export ALI_CLOUD_ACCESS_KEY_SECRET=<Your SK>
//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.MaxCompute2OSS \
//-f /home/ec2-user/20240403/ \
//-g maxcompute -d mc_2_hive -r cn-hangzhou \
//-l oss://oss-cn-hangzhou-internal.aliyuncs.com/xudalei-demo/external \
//-e "dh>='2024031115' and dh<='2024031820'"

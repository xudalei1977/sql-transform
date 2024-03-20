package com.aws.analytics

import org.slf4j.{Logger, LoggerFactory}
import com.aws.analytics.config.DBConfig
import com.aws.analytics.util._
import com.aws.analytics.config.InternalConfs
import com.aws.analytics.config.InternalConfs.InternalConfig

import java.io._

object MaxCompute2OSS {

  private val logger: Logger = LoggerFactory.getLogger(MaxCompute2OSS.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("export maxcompute data to oss ...")
    val params = DBConfig.parseConfig(MaxCompute2OSS, args)
    println(s"params := ${params.toString}")

    val util = new MaxcomputeUtil2(params.region, params.database)
    util.listTables().forEach(t=>{
      //util.unloadToOSS(t.getName, params.ossUrl)
      util.createAndInsertExternalTable(t.getName, params.ossUrl, params.ossFilter)
    })
  }
}

//export ALI_CLOUD_ACCESS_KEY_ID=<Your AK>
//export ALI_CLOUD_ACCESS_KEY_SECRET=<Your SK>
//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.MaxCompute2OSS \
//-g maxcompute -d mc_2_hive -r cn-hangzhou \
//-l oss://oss-cn-hangzhou-internal.aliyuncs.com/xudalei-demo/external

package com.aws.analytics

import org.slf4j.{Logger, LoggerFactory}

import util.MaxcomputeUtil


object MaxComputeTableSQL {

  private val logger: Logger = LoggerFactory.getLogger(MaxComputeTableSQL.getClass)

  def main(args: Array[String]): Unit = {
    //    logger.info("start the create table sql script transform ...")
    val util = new MaxcomputeUtil("", "", "cn-hangzhou", "kentys_test")
    util.listTables().forEach(t=>{
      println(util.getTableDDL(t.getName))
    })
  }
}
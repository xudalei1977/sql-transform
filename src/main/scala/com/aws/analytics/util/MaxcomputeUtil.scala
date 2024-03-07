package com.aws.analytics.util

import com.aliyun.maxcompute20220104.Client
import com.aliyun.maxcompute20220104.models.ListTablesResponseBody.ListTablesResponseBodyDataTables
import com.aliyun.maxcompute20220104.models.{GetTableInfoRequest, ListTablesRequest, ListTablesResponseBody}
import com.aliyun.teaopenapi.models.Config
import com.aliyun.teautil.models.RuntimeOptions
import com.aws.analytics.MaxComputeTableSQL
import org.slf4j.{Logger, LoggerFactory}

import java.util.HashMap
import java.util.List

class MaxcomputeUtil(ak: String, sk: String, region: String, _projectName: String) {
  private val logger: Logger = LoggerFactory.getLogger(MaxComputeTableSQL.getClass)

  private val config = new Config().setAccessKeyId(ak).setAccessKeySecret(sk).setRegionId(region)
  private val client: Client = new Client(config)
  private val projectName = _projectName

  def listTables(): List[ListTablesResponseBodyDataTables] = {
    val runtime = new RuntimeOptions()
    val listTablesRequest = new ListTablesRequest()
    val headers = new HashMap[String, String]
    val listTablesResponse = client.listTablesWithOptions(projectName, listTablesRequest, headers, runtime)

    listTablesResponse.getBody.getData.getTables
  }

  def getTableDDL(tableName: String): String = {
    val runtime = new RuntimeOptions()
    val getTableInfoRequest = new GetTableInfoRequest()
    val headers = new HashMap[String, String]
    val getTableInfoResponse = client.getTableInfoWithOptions(projectName, tableName, getTableInfoRequest, headers, runtime)
    getTableInfoResponse.getBody.getData.createTableDDL
  }
}

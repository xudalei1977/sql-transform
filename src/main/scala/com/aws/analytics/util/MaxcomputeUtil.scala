package com.aws.analytics.util

import com.aliyun.maxcompute20220104.Client
import com.aliyun.maxcompute20220104.models.ListTablesResponseBody.ListTablesResponseBodyDataTables
import com.aliyun.maxcompute20220104.models.{GetTableInfoRequest, ListTablesRequest, ListTablesResponseBody}
import com.aliyun.teaopenapi.models.Config
import com.aliyun.teautil.models.RuntimeOptions
import org.slf4j.{Logger, LoggerFactory}

import java.util.HashMap
import java.util.List

class MaxcomputeUtil(accessID: String, accessKey: String, region: String, _projectName: String, _s3Location: String) {
  private val logger: Logger = LoggerFactory.getLogger("MaxcomputeUtil")

  private val config = new Config().setAccessKeyId(accessID).setAccessKeySecret(accessKey).setRegionId(region)
  private val client: Client = new Client(config)
  private val projectName = _projectName
  private val s3Location = _s3Location

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
    var createTableSQL = getTableInfoResponse.getBody.getData.createTableDDL

    //remove the tblproperties
    //val p1 = """(?i)\btblproperties\b\s+\(([a-z0-9_\.,'"\=\s*]+)\)""".r
    val p1 = """(?i)\btblproperties\b\s+\((.*)\)""".r
    createTableSQL = p1.replaceAllIn("", createTableSQL)

    //remove the lifecycle
    val p2 = """(?i)\blifecycle\b\s+\([1-9]\)""".r
    createTableSQL = p1.replaceAllIn("", createTableSQL)

    //add s3 location
    createTableSQL += s"\n location $s3Location/$tableName"
    createTableSQL
  }
}

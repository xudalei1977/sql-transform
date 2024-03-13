package com.aws.analytics.util

import com.aliyun.maxcompute20220104.Client
import com.aliyun.maxcompute20220104.models.ListTablesResponseBody.ListTablesResponseBodyDataTables
import com.aliyun.maxcompute20220104.models.{GetTableInfoRequest, ListTablesRequest}
import com.aliyun.teaopenapi.models.Config
import com.aliyun.teautil.models.RuntimeOptions
import org.slf4j.{Logger, LoggerFactory}

import java.util.{HashMap, List}

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

        //drop last ";"
        createTableSQL = createTableSQL.trim
        if (createTableSQL.endsWith(";"))
          createTableSQL = createTableSQL.dropRight(1)

        //add external table
        val p1 = """(?i)\bcreate\b\s+\btable\b\s+""".r
        createTableSQL = p1.replaceFirstIn(createTableSQL, "create external table ")

        //remove the tblproperties
        //val p1 = """(?i)\btblproperties\b\s+\(([a-z0-9_\.,'"\=\s*]+)\)""".r
        val p2 = """(?i)\btblproperties\b\s+\((.*)\)""".r
        createTableSQL = p2.replaceFirstIn(createTableSQL, "")

        //remove the lifecycle
        val p3 = """(?i)\blifecycle\b\s+[1-9]+""".r
        createTableSQL = p3.replaceFirstIn(createTableSQL, "")

        //remove the store format
        val p4 = """(?i)\bstored\b\s+\bas\b\s+([a-z]+)""".r
        createTableSQL = p4.replaceFirstIn(createTableSQL, "")

        //remove the location
        val p5 = """(?i)\blocation\b\s+'(.*)'""".r
        createTableSQL = p5.replaceFirstIn(createTableSQL, "")

        //add stored format
        createTableSQL += s" stored as parquet \n"

        //add s3 location
        createTableSQL += s" location '$s3Location/$tableName/';"
        createTableSQL
    }
}

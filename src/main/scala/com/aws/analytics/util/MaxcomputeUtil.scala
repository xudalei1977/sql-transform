package com.aws.analytics.util

import com.aliyun.maxcompute20220104.Client
import com.aliyun.maxcompute20220104.models.ListTablesResponseBody.ListTablesResponseBodyDataTables
import com.aliyun.maxcompute20220104.models.{GetTableInfoRequest, ListTablesRequest, GetTableInfoResponseBody}
import com.aliyun.teaopenapi.models.Config
import com.aliyun.teautil.models.RuntimeOptions
import com.aws.analytics.config.DBConfig
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Connection
import java.sql.DriverManager
import java.util.{HashMap, List, Properties}

class MaxcomputeUtil(region: String, projectName: String) {
    private val logger: Logger = LoggerFactory.getLogger("MaxcomputeUtil")
    private val accessID = System.getenv("ALI_CLOUD_ACCESS_KEY_ID")
    private val accessKey = System.getenv("ALI_CLOUD_ACCESS_KEY_SECRET")

    private val config = new Config().setAccessKeyId(accessID).setAccessKeySecret(accessKey).setRegionId(region)
    private val client: Client = new Client(config)

    def getJDBCUrl(): String = {
        s"jdbc:odps:http://service.$region.maxcompute.aliyun.com/api?project=$projectName&useProjectTimeZone=true;"
    }

    def getConnection(): Connection = {
        val connectionString = getJDBCUrl()
        println(s"connection string: = ${connectionString}" )
        Class.forName("com.aliyun.odps.jdbc.OdpsDriver")
        DriverManager.getConnection(connectionString, accessID, accessKey)
    }

    def listTables(): List[ListTablesResponseBodyDataTables] = {
        val runtime = new RuntimeOptions()
        val listTablesRequest = new ListTablesRequest()
        val headers = new HashMap[String, String]
        val listTablesResponse = client.listTablesWithOptions(projectName, listTablesRequest, headers, runtime)

        listTablesResponse.getBody.getData.getTables
    }

    def getTableDDL(tableName: String, s3Location: String): (String, String) = {
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

        //add stored format, location, and snappy
        createTableSQL +=
          s""" stored as parquet
             | location '$s3Location/$tableName'
             | tblproperties('parquet.compression' = 'SNAPPY')""".stripMargin

        //process the partition columns
        import scala.collection.JavaConverters._
        val partitionColumnList:List[GetTableInfoResponseBody.GetTableInfoResponseBodyDataPartitionColumns] = getTableInfoResponse.getBody.getData.getPartitionColumns
        val partitionColumns =
            if (partitionColumnList != null)
                partitionColumnList.asScala.toList.map(t=>t.getName).mkString(",")
            else
                ""
        (createTableSQL, partitionColumns)
    }

    def unloadToOSS(tableName: String, ossUrl: String): Unit = {
        try {
            val conn = getConnection()
            val stmt = conn.createStatement
            stmt.execute("set odps.stage.mapper.split.size=256")
            stmt.execute("set odps.sql.allow.fullscan=true")
            val unloadSQL = s"""
                              |unload from $tableName
                              |into
                              |location '$ossUrl'
                              |row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                              |with serdeproperties ('odps.properties.rolearn'='acs:ram::1480826946867828:role/aliyunodpsdefaultrole')
                              |stored as parquet
                              |properties('mcfed.parquet.compression'='SNAPPY')
                              |""".stripMargin
            stmt.execute(unloadSQL)
            stmt.close
            conn.close
        } catch {
            case e: Exception =>
                logger.error(e.toString)
                System.exit(1)
        }
    }

    def createAndInsertExternalTable(tableName: String, ossUrl: String, ossFilter: String): Unit = {

        var (createTableSQL, partitionColumns) = getTableDDL(tableName, ossUrl)

        //add external postfix to table name
        val p1 = """(?i)create\s+external\s+table\s+if\s+not\s+exists\s+([a-z0-9\._-]+)\s*\(""".r
        createTableSQL = p1.replaceFirstIn(createTableSQL, s"create external table $projectName.${tableName}_external ( ")
        logger.info(s"******* $createTableSQL")
        val conn = getConnection()
        val stmt = conn.createStatement()
        stmt.execute(createTableSQL)

        stmt.execute("set odps.sql.allow.fullscan=true")

        val insertSQL =
            if (partitionColumns != None && partitionColumns != "")
                s"""insert into $projectName.${tableName}_external partition($partitionColumns) select * from $projectName.${tableName} where ${ossFilter}"""
            else
                s"""insert into $projectName.${tableName}_external select * from $projectName.${tableName} where ${ossFilter}"""
        logger.info(s"******* $insertSQL")
        stmt.execute(insertSQL)

        stmt.close()
    }
}

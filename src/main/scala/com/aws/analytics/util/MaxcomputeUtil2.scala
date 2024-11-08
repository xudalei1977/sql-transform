package com.aws.analytics.util

import com.aliyun.maxcompute20220104.Client
import com.aliyun.maxcompute20220104.models.ListTablesResponseBody.ListTablesResponseBodyDataTables
import com.aliyun.maxcompute20220104.models.{GetTableInfoRequest, GetTableInfoResponseBody, ListTablesRequest}
import com.aliyun.teaopenapi.models.Config
import com.aliyun.teautil.models.RuntimeOptions
import com.aws.analytics.config.DBConfig

import scala.collection.mutable._
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.{HashMap, List, Properties}
import scala.collection.immutable.Seq

class MaxcomputeUtil2(region: String, projectName: String) {
    private val logger: Logger = LoggerFactory.getLogger("MaxcomputeUtil")
    private val MaxCompute_CLASS_NAME = "com.aliyun.odps.jdbc.OdpsDriver"
    private val accessID = System.getenv("ALI_CLOUD_ACCESS_KEY_ID")
    private val accessKey = System.getenv("ALI_CLOUD_ACCESS_KEY_SECRET")

    private val config = new Config().setAccessKeyId(accessID).setAccessKeySecret(accessKey).setEndpoint(s"maxcompute.$region.aliyuncs.com")
    private val client: Client = new Client(config)

    def getJDBCUrl(): String = {
        s"jdbc:odps:http://service.$region.maxcompute.aliyun.com/api?project=$projectName&useProjectTimeZone=true;"
    }

    def getJDBCUrl(conf: DBConfig): String = {
        s"jdbc:odps:http://service.$region.maxcompute.aliyun.com/api?project=$projectName&useProjectTimeZone=true;"
    }

    def queryByJDBC(conf: DBConfig, sql: String) : Seq[String] = {
        var conn: Connection = null
        var ps: PreparedStatement = null
        var rs: ResultSet = null
        var seq: Seq[String] = Seq()
        try {
            conn = getConnection(conf)
            ps = conn.prepareStatement(sql)
            rs = ps.executeQuery

            while (rs.next) {
                seq :+= rs.getString(1)
            }
            seq
        } catch {
            case e: Exception => e.printStackTrace
                seq
        } finally {
            if (rs != null) rs.close
            if (ps != null) ps.close
            if (conn != null) conn.close
        }
    }

    def getConnection(): Connection = {
        val connectionString = getJDBCUrl()
        println(s"connection string: = ${connectionString}" )
        Class.forName(MaxCompute_CLASS_NAME)
        DriverManager.getConnection(connectionString, accessID, accessKey)
    }

    def getConnection(conf: DBConfig): Connection = {
        val connectionString = getJDBCUrl(conf)
        println(s"connection string: = ${connectionString}" )
        Class.forName(MaxCompute_CLASS_NAME)
        DriverManager.getConnection(connectionString, accessID, accessKey)
    }

    //do NOT use this if many result.
    def listTables(): List[ListTablesResponseBodyDataTables] = {
        val runtime = new RuntimeOptions()
        val listTablesRequest = new ListTablesRequest()
        val headers = new HashMap[String, String]
        val listTablesResponse = client.listTablesWithOptions(projectName, listTablesRequest, headers, runtime)

        listTablesResponse.getBody.getData.getTables
    }

    def getTableDDL(tableName: String, s3Location: String): (String, String, Boolean) = {
        val runtime = new RuntimeOptions()
        val getTableInfoRequest = new GetTableInfoRequest()
        val headers = new HashMap[String, String]
        val getTableInfoResponse = client.getTableInfoWithOptions(projectName, tableName, getTableInfoRequest, headers, runtime)
        var createTableSQL = getTableInfoResponse.getBody.getData.createTableDDL
        println(s"****** $createTableSQL")

        //drop last ";"
        createTableSQL = createTableSQL.trim
        if (createTableSQL.endsWith(";"))
          createTableSQL = createTableSQL.dropRight(1)

        //check if external table
        val p1 = """(?i)\bcreate\b\s+\bexternal\b\s+\btable\b\s+""".r
        val isExternal = if(p1.findFirstMatchIn(createTableSQL).getOrElse("").toString.equals("")) false else true

        //replace datetime to date
        createTableSQL = createTableSQL.replaceAll(s"(?i)datetime", "date")

        //add external table
        val p2 = """(?i)\bcreate\b\s+\btable\b\s+""".r
        createTableSQL = p2.replaceFirstIn(createTableSQL, "create external table ")

        //get the comments from tblproperties
        //val p3 = """(?i)\btblproperties\b\s+\(([a-z0-9_\.,'"\=\s*]+)\)""".r
        val p3 = """(?i)\btblproperties\b\s+\((.*)\)""".r
        val tblproperties = (for (m <- p3.findFirstMatchIn(createTableSQL)) yield m.group(1)).getOrElse("")

        val p4 = """(?i)'comment'\s*=\s*'(.*)'""".r
        val comment = (for (m <- p4.findFirstMatchIn(tblproperties)) yield m.group(1)).getOrElse("")

        //remove the tblproperties
        createTableSQL = p3.replaceFirstIn(createTableSQL, "")

        //get the lifecycle
        val p5 = """(?i)\blifecycle\b\s+([+-]?[0-9]+)""".r
        val lifecycle = (for (m <- p5.findFirstMatchIn(createTableSQL)) yield m.group(1)).getOrElse("3650")
        createTableSQL = p5.replaceFirstIn(createTableSQL, "")

        //remove the store format
        val p6 = """(?i)\bstored\b\s+\bas\b\s+([a-z]+)""".r
        createTableSQL = p6.replaceFirstIn(createTableSQL, "")

        //remove the location
        val p7 = """(?i)\blocation\b\s+'(.*)'""".r
        createTableSQL = p7.replaceFirstIn(createTableSQL, "")

        //add stored format, location, and snappy
        createTableSQL +=
          s""" stored as parquet
             | location '$s3Location/$projectName/$tableName'
             | tblproperties('comment' = '$comment',
             | 'lifecycle' = '$lifecycle',
             | 'owner' = '',
             | 'parquet.compression' = 'SNAPPY');""".stripMargin

        //process the partition columns
        import scala.collection.JavaConverters._
        val partitionColumnList:List[GetTableInfoResponseBody.GetTableInfoResponseBodyDataPartitionColumns] = getTableInfoResponse.getBody.getData.getPartitionColumns
        val partitionColumns =
            if (partitionColumnList != null)
                partitionColumnList.asScala.toList.map(t=>t.getName).mkString(",")
            else
                ""
        if (partitionColumns != "")
            createTableSQL += s"\n\n msck repair table $projectName.$tableName ; \n"

        (createTableSQL, partitionColumns, isExternal)
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

    def createAndInsertExternalTable(tableName: String, ossUrl: String, ossFilter: String): (String, Boolean) = {

        var (createTableSQL, partitionColumns, isExternal) = getTableDDL(tableName, ossUrl)

        //ignore external table
        if(isExternal)
            return (s"---------- ignore external table ${tableName} ----------\n", isExternal)

        //add external postfix to table name
        val p1 = """(?i)create\s+external\s+table\s+if\s+not\s+exists\s+([a-z0-9\._-]+)\s*\(""".r
        createTableSQL = p1.replaceFirstIn(createTableSQL, s"create external table if not exists $projectName.${tableName}_external ( ")
        logger.info(s"******* $createTableSQL")

        //check the filter, the filter is dh='YYYYMMDDHI', we convert it to dt='YYYYMMDD' for table use dt instead of dh as partition
        //if filter is dh>'2024010100' and dh<'2024032100', and table use dt as partition, we convert in to multiple insert sentence with dt
        var dh_or_dt = ""
        partitionColumns.split(",").foreach( column => {
            if(column.trim.equalsIgnoreCase("dh"))
                dh_or_dt = "dh"
            else if(column.trim.equalsIgnoreCase("dt"))
                dh_or_dt = "dt"
        })

        var dayArray = new ListBuffer[String]()
        var and_1 = ""
        var and_2 = ""
        if (ossFilter.toLowerCase.indexOf("and") > 0) {
            val p_dh = """(?i)dh[>=]+'([0-9]+)'\s+and\s+dh[<=]+'([0-9]+)'""".r
            val (start, end) = (for (m <- p_dh.findFirstMatchIn(ossFilter)) yield (m.group(1),m.group(2))).getOrElse(("", ""))
            dayArray = getDayInStringArray(start.substring(0, start.length-2), end.substring(0, end.length-2))

            and_1= ossFilter.toLowerCase.split("and")(0)
            and_2= ossFilter.toLowerCase.split("and")(1)
        }

        val filters = new ListBuffer[String]()
        if(dh_or_dt.equals("dh")) {
            if (ossFilter.toLowerCase.indexOf("and") > 0 && dayArray.length > 1) {
                dayArray.foreach(day => filters.append(s"dh like'$day%'"))
                filters(0) = filters(0) + " and " + and_1
                filters(filters.length-1) = filters(filters.length-1) + " and " + and_2
            } else {
                filters.append(ossFilter)
            }
        } else {
            if (ossFilter.toLowerCase.indexOf("and") > 0 && dayArray.length > 1) {
                dayArray.foreach(day => filters.append(s"dt='$day'"))
            } else {
                filters.append("dt" + ossFilter.substring(2, ossFilter.length - 3) + "'")
            }
        }

        var insertSQL = ""
        if (partitionColumns != None && partitionColumns != "")
            filters.foreach(f => insertSQL += s"""insert overwrite table $projectName.${tableName}_external partition($partitionColumns) select * from $projectName.${tableName} where $f;\n""")
        else
            insertSQL += s"""insert overwrite table $projectName.${tableName}_external select * from $projectName.${tableName};\n"""

        logger.info(s"******* successful execution sql : $insertSQL")

        (createTableSQL + "\n\n" + insertSQL, isExternal)
    }


    /****
     * generate all days between start and end day.
    * */
    def getDayInStringArray(startDay: String, endDay: String): ListBuffer[String] = {
        val format = new java.text.SimpleDateFormat("yyyyMMdd")
        val startDayInDate = format.parse(startDay)
        import java.util.Calendar
        val cal = Calendar.getInstance
        cal.setTime(startDayInDate)
        cal.add(Calendar.DATE, 1)
        var nextDay = format.format(cal.getTime())

        val dayList = new ListBuffer[String]()
        dayList.append(startDay)
        while (nextDay <= endDay) {
            dayList.append(nextDay)
            cal.add(Calendar.DATE, 1)
            nextDay = format.format(cal.getTime())
        }

        dayList
    }
}

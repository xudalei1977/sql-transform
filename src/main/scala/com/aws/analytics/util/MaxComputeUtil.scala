package com.aws.analytics.util

import com.aws.analytics.config.InternalConfs.{DBField, InternalConfig, TableDetails}
import com.aws.analytics.config.{DBConfig, RedshiftType}
import org.slf4j.{Logger, LoggerFactory}

import java.sql._
import java.util.Properties
import scala.collection.immutable.{IndexedSeq, Seq, Set}


class MaxComputeUtil extends DBEngineUtil {
    private val logger: Logger = LoggerFactory.getLogger("MaxComputeUtil")
    private val MaxCompute_CLASS_NAME = "com.aliyun.odps.jdbc.OdpsDriver"
    private val accessID = System.getenv("ALI_CLOUD_ACCESS_KEY_ID")
    private val accessKey = System.getenv("ALI_CLOUD_ACCESS_KEY_SECRET")

    def getJDBCUrl(conf: DBConfig): String = {
        s"jdbc:odps:http://service.${conf.mcRegion}.maxcompute.aliyun.com/api?project=${conf.database}&useProjectTimeZone=true;"
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

    def getConnection(conf: DBConfig): Connection = {
        val connectionString = getJDBCUrl(conf)
        println(s"connection string: = ${connectionString}" )
        Class.forName(MaxCompute_CLASS_NAME)
        DriverManager.getConnection(connectionString, accessID, accessKey)
    }
    
    //Use this method to get the columns to extract
    def getValidFieldNames(conf: DBConfig, internalConfig: InternalConfig)(implicit crashOnInvalidType: Boolean): String = {
        val conn = getConnection(conf)
        val tableDetails = getTableDetails(conf, internalConfig)(crashOnInvalidType)
        conn.close()
        tableDetails.validFields.map(r => s""" ${r.fieldName.toLowerCase} """).mkString(",")
    }

    def getTableDetails(conf: DBConfig, internalConfig: InternalConfig)(implicit crashOnInvalidType: Boolean): TableDetails = {
        val conn = getConnection(conf)
        val stmt = conn.createStatement()
        val query = s"SELECT * from ${conf.database}.${conf.tableName} where 1 < 0"
        val rs = stmt.executeQuery(query)
        val rsmd = rs.getMetaData
        var validFields = Seq[DBField]()
        var invalidFields = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()

        for (i <- 1 to rsmd.getColumnCount) {
            val columnType = rsmd.getColumnTypeName(i)
            val precision = rsmd.getPrecision(i)
            val scale = rsmd.getScale(i)
            val comment = rsmd.getColumnLabel(i)

            validFields = validFields :+ DBField(rsmd.getColumnName(i), columnType, None)

//            if (validFieldTypes.contains(columnType.toUpperCase)) {
//                val redshiftColumnType = convertPostgresSQLTypeToRedshiftType(columnType, precision, scale)
//                val javaTypeMapping = {
//                    if (redshiftColumnType == "TIMESTAMP" || redshiftColumnType == "DATE") Some("String")
//                    else None
//                }
//                validFields = validFields :+ DBField(rsmd.getColumnName(i), redshiftColumnType, javaTypeMapping)
//
//            } else {
//                if (crashOnInvalidType)
//                    throw new IllegalArgumentException(s"Invalid type $columnType")
//                invalidFields = invalidFields :+ DBField(rsmd.getColumnName(i), columnType)
//            }

            setColumns = setColumns + rsmd.getColumnName(i).toLowerCase
            logger.info(s" column: ${rsmd.getColumnName(i)}, type: ${rsmd.getColumnTypeName(i)}," +
              s" comments: ${rsmd.getPrecision(i)}\n")
        }
        rs.close()
        stmt.close()
        val sortKeys = getIndexes(conn, setColumns, conf)
        val distKey = getDistStyleAndKey(conn, setColumns, conf, internalConfig)
        val primaryKey = getPrimaryKey(conn, setColumns, conf)
        conn.close()
        TableDetails(validFields, invalidFields, sortKeys, distKey, primaryKey)
    }

    def getIndexes(conn: Connection, setColumns: Set[String], conf: DBConfig): IndexedSeq[String] = {
        val meta = conn.getMetaData
        val resIndexes = meta.getIndexInfo(conf.database, conf.schema, conf.tableName, false, false)
        var setIndexedColumns = scala.collection.immutable.Set[String]()
        while (resIndexes.next) {
            val columnName = resIndexes.getString(9)
            if (setColumns.contains(columnName.toLowerCase)) {
                setIndexedColumns = setIndexedColumns + columnName
            } else {
                System.err.println(s"Rejected $columnName")
            }
        }
        resIndexes.close()
        // Redshift can only have 8 interleaved sort keys
        setIndexedColumns.toIndexedSeq.take(2)
    }

    def getDistStyleAndKey(con: Connection, setColumns: Set[String], conf: DBConfig, internalConfig: InternalConfig): Option[String] = {
        internalConfig.distKey match {
            case Some(key) =>
                logger.info("Found distKey in configuration {}", key)
                Some(key)
            case None =>
                logger.info("Found no distKey in configuration")
                val meta = con.getMetaData
                val resPrimaryKeys = meta.getPrimaryKeys(conf.database, conf.schema, conf.tableName)
                var primaryKeys = scala.collection.immutable.Set[String]()

                while (resPrimaryKeys.next) {
                    val columnName = resPrimaryKeys.getString(4)
                    if (setColumns.contains(columnName.toLowerCase)) {
                        primaryKeys = primaryKeys + columnName
                    } else {
                        logger.warn(s"Rejected $columnName")
                    }
                }

                resPrimaryKeys.close()
                if (primaryKeys.size != 1) {
                    logger.error(s"Found multiple or zero primary keys, Not taking any. ${primaryKeys.mkString(",")}")
                    None
                } else {
                    logger.info(s"Found primary keys, distribution key is. ${primaryKeys.toSeq.head}")
                    Some(primaryKeys.toSeq.head)
                }
        }
    }

    def getPrimaryKey(conn: Connection, setColumns: Set[String], conf: DBConfig): Option[String] = {
        val meta = conn.getMetaData
        val resPrimaryKeys = meta.getPrimaryKeys(conf.database, null, conf.tableName)
        var primaryKeys = scala.collection.immutable.Set[String]()

        while (resPrimaryKeys.next) {
            val columnName = resPrimaryKeys.getString(4)
            if (setColumns.contains(columnName.toLowerCase)) {
                primaryKeys = primaryKeys + columnName
            } else {
                logger.warn(s"Rejected $columnName")
            }
        }

        resPrimaryKeys.close()
        if (primaryKeys.size > 1) {
            Some(primaryKeys.mkString(","))
        } else if (primaryKeys.size == 1)  {
            Some(primaryKeys.toSeq.head)
        } else {
            None
        }
    }


    /**
     * transfer 1:
     * transfer 2:
     * transfer 3:
     */
    def transferDateFunction(sql:String): String = {
        var newSql = sql
        //todo: add transfer
        newSql
    }

    /**
     * transfer 1:
     * transfer 2:
     * transfer 3:
     */
    def transferCharFunction(sql:String): String = {
        var newSql = sql
        //todo: add transfer
        newSql
    }

    /**
     * get create table in hive
     * *
     * do NOT use this one, because the show create table could not be executed
     */
    def getCreateTableSQL(conf: DBConfig): (String, String, Boolean) = {
        val query = s"show create table ${conf.database}.${conf.tableName}"
        var createTableSQL = queryByJDBC(conf, query)(0)

        //drop last ";"
        createTableSQL = createTableSQL.trim
        if (createTableSQL.endsWith(";"))
            createTableSQL = createTableSQL.dropRight(1)

        //check if external table
        val p1 = """(?i)\bcreate\b\s+\bexternal\b\s+\btable\b\s+""".r
        val isExternal = if(p1.findFirstMatchIn(createTableSQL).getOrElse("").toString.equals("")) false else true

        //add external table, because in aws emr, table are all external
        val p2 = """(?i)\bcreate\b\s+\btable\b\s+""".r
        createTableSQL = p2.replaceFirstIn(createTableSQL, "create external table ")

        //remove the tblproperties
        //val p3 = """(?i)\btblproperties\b\s+\(([a-z0-9_\.,'"\=\s*]+)\)""".r
        val p3 = """(?i)\btblproperties\b\s+\((.*)\)""".r
        createTableSQL = p3.replaceFirstIn(createTableSQL, "")

        //remove the lifecycle
        val p4 = """(?i)\blifecycle\b\s+[1-9]+""".r
        createTableSQL = p4.replaceFirstIn(createTableSQL, "")

        //remove the store format
        val p5 = """(?i)\bstored\b\s+\bas\b\s+([a-z]+)""".r
        createTableSQL = p5.replaceFirstIn(createTableSQL, "")

        //remove the location
        val p6 = """(?i)\blocation\b\s+'(.*)'""".r
        createTableSQL = p6.replaceFirstIn(createTableSQL, "")

        //add stored format, location, and snappy
        createTableSQL +=
          s""" stored as parquet
             | location '${conf.hiveInS3Path}/${conf.database}/${conf.tableName}'
             | tblproperties('parquet.compression' = 'SNAPPY')""".stripMargin

        val p7 = """(?i)\bpartitioned\b\s+by\s+\((.*)\)""".r
        val partitionStr = p7.findFirstMatchIn(createTableSQL).getOrElse("").toString
        var partitionColumnStr = ""
        if (partitionStr != "") {
            val partitionColumnArr = partitionStr.substring(partitionStr.indexOf("(")+1, partitionStr.indexOf(")")).split(",")
            partitionColumnStr = (for(partitionColumn <- partitionColumnArr) yield partitionColumn.trim.split(" ")(0).trim).mkString(",")
        }

        (createTableSQL, partitionColumnStr, isExternal)
    }

    def createAndInsertExternalTable(conf: DBConfig): Unit = {
        var (createTableSQL, partitionColumns, isExternal) = getCreateTableSQL(conf)

        //ignore external table
        if(isExternal) return

        //add external postfix to table name
        val p1 = """(?i)create\s+external\s+table\s+if\s+not\s+exists\s+([a-z0-9\._-]+)\s*\(""".r
        createTableSQL = p1.replaceFirstIn(createTableSQL, s"create external table ${conf.database}.${conf.tableName}_external ( ")
        logger.info(s"******* $createTableSQL")
        val conn = getConnection(conf)
        val stmt = conn.createStatement()
        stmt.execute(createTableSQL)

        stmt.execute("set odps.sql.allow.fullscan=true")
        stmt.execute("set odps.sql.hive.compatible=true")
        stmt.execute("set odps.sql.unstructured.oss.commit.mode=true")
        stmt.execute("set odps.sql.unstructured.file.pattern.black.list=.*_SUCCESS$,.*.hive_staging.*")

        //check the filter, the filter is dh=YYYYMMDDHI, we convert it to dt=YYYYMMDD for table use dt instead of dh as partition
        val filter =
            if(partitionColumns.indexOf("dh") > 0)
                conf.ossFilter
            else if(partitionColumns.indexOf("dt") > 0)
                "dt=" + conf.ossFilter.substring(3, 11)
            else
                "1=1"

        val insertSQL =
            if (partitionColumns != None && partitionColumns != "")
                s"""insert overwrite ${conf.database}.${conf.tableName}_external partition($partitionColumns) select * from ${conf.database}.${conf.tableName} where ${filter}"""
            else
                s"""insert overwrite ${conf.database}.${conf.tableName}_external select * from {conf.database}.${conf.tableName}"""
        logger.info(s"******* $insertSQL")
        stmt.execute(insertSQL)

        stmt.close()
    }

}

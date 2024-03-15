package com.aws.analytics.util

import java.util.Properties
import java.util.regex._
import com.aws.analytics.config.DBConfig
import com.aws.analytics.config.Configurations.DBConfiguration
import com.aws.analytics.config.InternalConfs.{IncrementalSettings, InternalConfig, TableDetails,DBField}
import com.aws.analytics.config.RedshiftType
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.immutable.{IndexedSeq, Seq, Set}


class ADBMySQLUtil extends DBEngineUtil {
    private val logger: Logger = LoggerFactory.getLogger("ADBMySQLUtil")
    private val MySQL_CLASS_NAME = "com.mysql.jdbc.Driver"
    private val accessID = System.getenv("ALI_CLOUD_ACCESS_KEY_ID")
    private val accessKey = System.getenv("ALI_CLOUD_ACCESS_KEY_SECRET")

    def getJDBCUrl(conf: DBConfig): String = {
        s"jdbc:mysql://${conf.hostname}:${conf.portNo}/${conf.database}?useSSL=false&user=${conf.userName}&password=${conf.password}"
    }

    def queryByJDBC(conf: DBConfig, sql: String) : Seq[String] = {
        var conn: Connection = null
        var ps: PreparedStatement = null
        var rs: ResultSet = null
        var seq: Seq[String] = Seq()
        try {
            Class.forName(MySQL_CLASS_NAME)
            conn = DriverManager.getConnection(getJDBCUrl(conf))
            ps = conn.prepareStatement(sql)
            rs = ps.executeQuery

            while (rs.next)
                seq :+= rs.getString(1)
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
        val connectionProps = new Properties()
        //connectionProps.put("user", conf.userName)
        //connectionProps.put("password", conf.password)
        val connectionString = getJDBCUrl(conf)
        println(s"connection string: = ${connectionString}" )
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection(connectionString, connectionProps)
    }

    //Use this method to get the columns to extract
    def getValidFieldNames(mysqlConfig: DBConfig, internalConfig: InternalConfig)(implicit crashOnInvalidType: Boolean): TableDetails = {
        val conn = getConnection(mysqlConfig)
        val tableDetails = getTableDetails(conn, mysqlConfig, internalConfig)(crashOnInvalidType)
        conn.close()
        tableDetails
    }

    def getTableDetails(conn: Connection, conf: DBConfig, internalConfig: InternalConfig)
                       (implicit crashOnInvalidType: Boolean): TableDetails = {
        val stmt = conn.createStatement()
        val query = s"SELECT * from ${conf.database}.${conf.tableName} where 1 < 0"
        val rs = stmt.executeQuery(query)
        val rsmd = rs.getMetaData
        val validFieldTypes = mysqlToRedshiftTypeConverter.keys.toSet
        var validFields = Seq[DBField]()
        var invalidFields = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()

        for (i <- 1 to rsmd.getColumnCount) {
            val columnType = rsmd.getColumnTypeName(i)
            val precision = rsmd.getPrecision(i)
            val scale = rsmd.getScale(i)

            if (validFieldTypes.contains(columnType.toUpperCase)) {
                val redshiftColumnType = convertMySqlTypeToRedshiftType(columnType, precision, scale)
                val javaTypeMapping = {
                    if (redshiftColumnType == "TIMESTAMP" || redshiftColumnType == "DATE") Some("String")
                    else None
                }
                validFields = validFields :+ DBField(rsmd.getColumnName(i), redshiftColumnType, javaTypeMapping)

            } else {
                if (crashOnInvalidType)
                    throw new IllegalArgumentException(s"Invalid type $columnType")
                invalidFields = invalidFields :+ DBField(rsmd.getColumnName(i), columnType)
            }
            setColumns = setColumns + rsmd.getColumnName(i).toLowerCase
            logger.info(s" column: ${rsmd.getColumnName(i)}, type: ${rsmd.getColumnTypeName(i)}," +
              s" precision: ${rsmd.getPrecision(i)}, scale:${rsmd.getScale(i)}\n")
        }
        rs.close()
        stmt.close()
        val sortKeys = getIndexes(conn, setColumns, conf)
        val distKey = getDistStyleAndKey(conn, setColumns, conf, internalConfig)
        val primaryKey = getPrimaryKey(conn, setColumns, conf)
        TableDetails(validFields, invalidFields, sortKeys, distKey, primaryKey)
    }

    def convertMySqlTypeToRedshiftType(columnType: String, precision: Int, scale: Int): String = {
        val redshiftType: RedshiftType = mysqlToRedshiftTypeConverter(columnType.toUpperCase)
        //typeName:String, hasPrecision:Boolean = false, hasScale:Boolean = false, precisionMultiplier:Int
        val result = if (redshiftType.hasPrecision && redshiftType.hasScale) {
            s"${redshiftType.typeName}(${precision * redshiftType.precisionMultiplier}, $scale)"
        } else if (redshiftType.hasPrecision) {
            var redshiftPrecision = precision * redshiftType.precisionMultiplier
            if (redshiftPrecision < 0 || redshiftPrecision > 65535)
                redshiftPrecision = 65535
            s"${redshiftType.typeName}($redshiftPrecision)"
        } else {
            redshiftType.typeName
        }
        logger.info(s"Converted type: $columnType, precision: $precision, scale:$scale to $result")
        result
    }

    def getIndexes(con: Connection, setColumns: Set[String], conf: DBConfig): IndexedSeq[String] = {
        val meta = con.getMetaData
        val resIndexes = meta.getIndexInfo(conf.database, null, conf.tableName, false, false)
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
                if (primaryKeys.size != 1) {
                    logger.error(s"Found multiple or zero primary keys, Not taking any. ${primaryKeys.mkString(",")}")
                    None
                } else {
                    logger.info(s"Found primary keys, distribution key is. ${primaryKeys.toSeq.head}")
                    Some(primaryKeys.toSeq.head)
                }
        }
    }

    def getPrimaryKey(con: Connection, setColumns: Set[String], conf: DBConfig): Option[String] = {

        val meta = con.getMetaData
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

    val mysqlToRedshiftTypeConverter: Map[String, RedshiftType] = {
        val maxVarcharSize = 65535
        Map(
            "TINYINT" -> RedshiftType("INT2"),
            "TINYINT UNSIGNED" -> RedshiftType("INT2"),
            "SMALLINT" -> RedshiftType("INT2"),
            "SMALLINT UNSIGNED" -> RedshiftType("INT4"),
            "MEDIUMINT" -> RedshiftType("INT4"),
            "MEDIUMINT UNSIGNED" -> RedshiftType("INT4"),
            "INT" -> RedshiftType("INT4"),
            "INT UNSIGNED" -> RedshiftType("INT8"),
            "BIGINT" -> RedshiftType("INT8"),
            "BIGINT UNSIGNED" -> RedshiftType("INT8"), //Corner case indeed makes this buggy, Just hoping that it does not occure!
            "FLOAT" -> RedshiftType("FLOAT4"),
            "DOUBLE" -> RedshiftType("FLOAT8"),
            "DECIMAL" -> RedshiftType("FLOAT8"),
            "CHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 4),
            "VARCHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 4),
            "TINYTEXT" -> RedshiftType("VARCHAR(1024)"),
            "TEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "MEDIUMTEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "LONGTEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "BOOLEAN" -> RedshiftType("BOOLEAN"),
            "BOOL" -> RedshiftType("BOOLEAN"),
            "ENUM" -> RedshiftType("VARCHAR(255)"),
            "SET" -> RedshiftType("VARCHAR(255)"),
            "DATE" -> RedshiftType("DATE"),
            "TIME" -> RedshiftType("TIMESTAMP"),
            "DATETIME" -> RedshiftType("TIMESTAMP"),
            "TIMESTAMP" -> RedshiftType("TIMESTAMP"),
            "YEAR" -> RedshiftType("INT")
        )
    }

    def transferDateFunction(sql:String): String = {
        //todo:
        ""
    }

    def transferCharFunction(sql:String): String = {
        //todo:
        ""
    }

    def createAndInsertExternalTable(conf: DBConfig): Unit = {
        val conn = getConnection(conf)
        val stmt = conn.createStatement()
        var sql = s"show create table ${conf.database}.${conf.tableName}"
        val rs = stmt.executeQuery(sql)
        var createTableSQL = rs.getString(0)

        //get the partition column by regexp, check whether the external table support "PARTITION BY VALUE(date_format(l_shipdate, '%Y%m'))"
        //val p1 = """(?i)\bpartition\b\s+by\b\s+value\b\s*\(\b\s*date_format\b\s*\(([a-z0-9_\.,]+)\s*,""".r
        val p1 = """(?i)\bpartition\b\s+by\b\s+\(([a-z0-9_\.,\s*]+)\)""".r
        val partitionStr = p1.findFirstMatchIn(createTableSQL).getOrElse("").toString
        var partitionColumn = ""
        if (partitionStr != None)
            partitionColumn = partitionStr.substring(partitionStr.indexOf("(")+1, partitionStr.indexOf(")"))

        //remove the "cluster by" clause
        var index1 = createTableSQL.toLowerCase.indexOf("distribute by")
        if (index1 > 0)
            createTableSQL = createTableSQL.substring(0, index1)

        //remove the "partition by" clause
        index1 = createTableSQL.toLowerCase.indexOf("partition by")
        if (index1 > 0)
            createTableSQL = createTableSQL.substring(0, index1)

        //replace the table name by adding "_external"
        val p2 = "(?i)`db1`.`table1`"
        createTableSQL = createTableSQL.replaceFirst(s"(?i)`${conf.database}`.`${conf.tableName}`",
                                                    s"`${conf.database}`.`${conf.tableName}_external`")

        createTableSQL += s"""ENGINE='OSS'
        TABLE_PROPERTIES='{
            "endpoint":"${conf.ossEndpoint}",
            "url":"${conf.ossUrl}/${conf.database}/${conf.tableName}",
            "accessid":"${accessID}",
            "accesskey":"${accessKey}",
            "format":"${conf.ossFormat}",
            "partition_column":"${partitionColumn}"
        }'"""

        stmt.execute(createTableSQL)

        sql = s"submit job insert overwrite ${conf.database}.${conf.tableName}_external " +
                    s" select * from ${conf.database}.${conf.tableName} where ${conf.ossFilter}"
        stmt.execute(sql)

        rs.close()
        stmt.close()
    }

    /**
      * Alter table to add or delete columns in redshift table if any changes occurs in sql table
      *
      * @param tableDetails sql table details
      * @param redshiftConf redshift configuration
      * @return Query of add and delete columns from redshift table
      */
//    private def alterTableQuery(tableDetails: TableDetails, redshiftConf: DBConfiguration, customFields:Seq[String]): String = {
//
//        val redshiftTableName: String = RedshiftUtil.getTableNameWithSchema(redshiftConf)
//        try {
//            val mainTableColumnNames: Set[String] = RedshiftUtil.getColumnNamesAndTypes(redshiftConf).keys.toSet
//
//            // All columns name must be distinct other wise redshift load will fail
//            val stagingTableColumnAndTypes: Map[String, String] = tableDetails
//                    .validFields
//                    .map { td => td.fieldName.toLowerCase -> td.fieldType }
//                    .toMap
//
//            val stagingTableColumnNames: Set[String] = (stagingTableColumnAndTypes.keys ++ customFields).toSet
//            val addedColumns: Set[String] = stagingTableColumnNames -- mainTableColumnNames
//            val deletedColumns: Set[String] = mainTableColumnNames -- stagingTableColumnNames
//
//            val addColumnsQuery = addedColumns.foldLeft("\n") { (query, columnName) =>
//                query + s"""ALTER TABLE $redshiftTableName ADD COLUMN "$columnName" """ +
//                        stagingTableColumnAndTypes.getOrElse(columnName, "") + ";\n"
//            }
//
//            val deleteColumnQuery = deletedColumns.foldLeft("\n") { (query, columnName) =>
//                query + s"""ALTER TABLE $redshiftTableName DROP COLUMN "$columnName" ;\n"""
//            }
//
//            addColumnsQuery + deleteColumnQuery
//        } catch {
//            case e: Exception =>
//                logger.warn("Error occurred while altering table: \n{}", e.getStackTrace.mkString("\n"))
//                ""
//        }
//    }
}

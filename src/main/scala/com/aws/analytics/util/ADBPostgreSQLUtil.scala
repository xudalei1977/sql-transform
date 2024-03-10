package com.aws.analytics.util

import com.aws.analytics.config.InternalConfs.{DBField, InternalConfig, TableDetails}
import com.aws.analytics.config.{DBConfig, RedshiftType}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import scala.collection.immutable.{IndexedSeq, Seq, Set}

/*
--packages "org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-redshift_2.10:1.1.0,com.amazonaws:aws-java-sdk:1.7.4,mysql:mysql-connector-java:5.1.39"
--jars=<Some-location>/RedshiftJDBC4-1.1.17.1017.jar
*/


class ADBPostgreSQLUtil extends DBEngineUtil {
    private val logger: Logger = LoggerFactory.getLogger("ADBPostgreSQLUtil")
    private val PostgreSQL_CLASS_NAME = "org.postgresql.Driver"


    def getJDBCUrl(conf: DBConfig): String = {
        s"jdbc:postgresql://${conf.hostname}:${conf.portNo}/${conf.database}?ssl=false"
    }

    def queryByJDBC(conf: DBConfig, sql: String) : Seq[String] = {
        var conn: Connection = null
        var ps: PreparedStatement = null
        var rs: ResultSet = null
        var seq: Seq[String] = Seq()
        try {
            Class.forName(PostgreSQL_CLASS_NAME)
            conn = DriverManager.getConnection(getJDBCUrl(conf), conf.userName, conf.password)
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
        connectionProps.put("user", conf.userName)
        connectionProps.put("password", conf.password)
        Class.forName(PostgreSQL_CLASS_NAME)
        DriverManager.getConnection(getJDBCUrl(conf), connectionProps)
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
        val query = s"SELECT * from ${conf.database}.${conf.schema}.${conf.tableName} where 1 < 0"
        val rs = stmt.executeQuery(query)
        val rsmd = rs.getMetaData
        val validFieldTypes = pgToRedshiftTypeConverter.keys.toSet
        var validFields = Seq[DBField]()
        var invalidFields = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()

        for (i <- 1 to rsmd.getColumnCount) {
            val columnType = rsmd.getColumnTypeName(i)
            val precision = rsmd.getPrecision(i)
            val scale = rsmd.getScale(i)

            if (validFieldTypes.contains(columnType.toUpperCase)) {
                val redshiftColumnType = convertPostgresSQLTypeToRedshiftType(columnType, precision, scale)
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

    def convertPostgresSQLTypeToRedshiftType(columnType: String, precision: Int, scale: Int): String = {
        val redshiftType: RedshiftType = pgToRedshiftTypeConverter(columnType.toUpperCase())
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

    val pgToRedshiftTypeConverter: Map[String, RedshiftType] = {
        val maxVarcharSize = 65535
        Map(
            "INT8" -> RedshiftType("INT8"),
            "BIGSERIAL" -> RedshiftType("INT8"),
            "BIT" -> RedshiftType("INT2"),
            "VARBIT" -> RedshiftType("INT2"),
            "BOOL" -> RedshiftType("BOOLEAN"),
            "BOX" -> RedshiftType("VARCHAR(256)"),
            "BYTEA" -> RedshiftType("VARCHAR(64)"),
            "BPCHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 1),
            "CHAR" -> RedshiftType("CHAR", hasPrecision = true, hasScale = false, 1),
            "VARCHAR" -> RedshiftType("VARCHAR", hasPrecision = true, hasScale = false, 1),
            "CIDR" -> RedshiftType("VARCHAR(64)"),
            "CIRCLE" -> RedshiftType("VARCHAR(64)"),
            "DATE" -> RedshiftType("DATE"),
            "NUMERIC" -> RedshiftType("NUMERIC", hasPrecision = true, hasScale = true, 1),
            "FLOAT8" -> RedshiftType("FLOAT8"),
            "INET" -> RedshiftType("VARCHAR(32)"),
            "INT4" -> RedshiftType("INT4"),
            "INTERVAL" -> RedshiftType("INT8"),
            "JSON" -> RedshiftType("SUPER"),
            "LSEQ" -> RedshiftType("VARCHAR(256)"),
            "MACADDR" -> RedshiftType("VARCHAR(64)"),
            "MONEY" -> RedshiftType("VARCHAR(64)"),
            "PATH" -> RedshiftType("VARCHAR(256)"),
            "POINT" -> RedshiftType("VARCHAR(256)"),
            "POLYGON" -> RedshiftType("VARCHAR(256)"),
            "FLOAT4" -> RedshiftType("FLOAT4"),
            "SERIAL" -> RedshiftType("FLOAT4"),
            "INT2" -> RedshiftType("INT2"),
            "TEXT" -> RedshiftType(s"VARCHAR($maxVarcharSize)"),
            "TIME" -> RedshiftType("TIME"),
            "TIMETZ" -> RedshiftType("TIMETZ"),
            "TIMESTAMP" -> RedshiftType("TIMESTAMP"),
            "TIMESTAMPTZ" -> RedshiftType("TIMESTAMPTZ"),
            "XML" -> RedshiftType("INT2"),
            "UUID" -> RedshiftType("VARCHAR(64)")
        )
    }

    /**
     * transfer 1: {Column} between date '{YYYY-MM_DD}' and date '{YYYY-MM-DD}' --> {Column} >= to_date('{YYYY-MM-DD}') and {Column} <= to_date('{YYYY-MM-DD}')
     * transfer 2: date '{YYYY-MM_DD}' - interval '93' {day|month|year} --> dateadd({day|month|year}, -93, '{YYYY-MM_DD}')
     * transfer 3: date '1994-08-01' --> to_date('1994-08-01')
     *
     *  between 0.06 - 0.01 and 0.06 + 0.01
     */
    def transferDateFunction(sql:String): String = {
        var newSql = ""

        val p1 = """(?i)\b[a-z0-9_\.]+\b\s+\bbetween\b\s+date\b\s+'([\d]{4})-([\d]{2})-([\d]{2})'\s+\band\b\s+\bdate\b\s+'([\d]{4})-([\d]{2})-([\d]{2})'""".r
        p1.findAllIn(sql).toList.foreach( funcStr => {
            val arr = funcStr.split(" ")
            val subFuncStr = s" ${arr(0)} >= to_date(${arr(3)}) and ${arr(0)} <= to_date(${arr(6)}) "
            newSql = sql.substring(0, sql.indexOf(funcStr)) + subFuncStr + sql.substring(sql.indexOf(funcStr) + funcStr.length)
        })
        if (newSql == "") newSql = sql

        val p2 = """(?i)\bdate\b\s+'([\d]{4})-([\d]{2})-([\d]{2})'\s+[-|+]\s+\binterval\b\s+'\s*[0-9]+'\s*(day|month|year)""".r
        p2.findAllIn(newSql).toList.foreach( funcStr => {
            val arr = funcStr.replace("'", "").split(" ")
            val subFuncStr = s" dataadd(${arr(5)}, ${arr(2)}${arr(4)}, '${arr(1)}') "
            newSql = newSql.substring(0, newSql.indexOf(funcStr)) + subFuncStr + newSql.substring(newSql.indexOf(funcStr) + funcStr.length)
        })
        if (newSql == "") newSql = sql

        val p3 = """(?i)\bdate\b\s+'([\d]{4})-([\d]{2})-([\d]{2})'""".r
        p3.findAllIn(newSql).toList.foreach( funcStr => {
            val arr = funcStr.split(" ")
            val subFuncStr = s" to_date(${arr(1)}) "
            newSql = newSql.substring(0, newSql.indexOf(funcStr)) + subFuncStr + newSql.substring(newSql.indexOf(funcStr) + funcStr.length)
        })
        if (newSql == "") newSql = sql

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
            "accessid":"${conf.accessID}",
            "accesskey":"${conf.accessKey}",
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

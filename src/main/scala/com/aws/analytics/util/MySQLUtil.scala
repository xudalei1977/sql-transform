package com.aws.analytics.util

import org.slf4j.{Logger, LoggerFactory}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import scala.collection.immutable.{IndexedSeq, Seq, Set}
import com.aws.analytics.config.DBConfig
import com.aws.analytics.config.InternalConfs._

class MySQLUtil extends DBEngineUtil {
    private val logger: Logger = LoggerFactory.getLogger("MySQLUtil")
    private val CLASS_NAME = "com.mysql.cj.jdbc.Driver"

    def getJDBCUrl(conf: DBConfig): String = {
        s"jdbc:mysql://${conf.hostname}:3306/${conf.database}?useSSL=false&tinyInt1isBit=false&user=${conf.userName}&password=${conf.password}"
    }
    
    def queryByJDBC(conf: DBConfig, sql: String) : Seq[String] = {
        var conn: Connection = null
        var ps: PreparedStatement = null
        var rs: ResultSet = null
        var seq: Seq[String] = Seq()
        try {
            Class.forName(CLASS_NAME)
            // logger.info(s"******** getJDBCUrl(conf) := ${getJDBCUrl(conf)}")
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
        val connectionString = getJDBCUrl(conf)
        println(s"connection string: = ${connectionString}" )
        Class.forName(CLASS_NAME)
        DriverManager.getConnection(connectionString)
    }

    //Use this method to get the columns to extract
    def getValidFieldNames(conf: DBConfig, tableName: String): String = {
        val conn = getConnection(conf)
        val tableDetails = getTableDetails(conf, null)(false)
        conn.close()
        tableDetails.validFields.map(r => s""" ${r.fieldName.toLowerCase} """).mkString(",")
    }

    def getTableDetails(conf: DBConfig, internalConfig: InternalConfig)(implicit crashOnInvalidType: Boolean): TableDetails = {
        logger.info(s"************************ tableName: ${conf.tableName}")
        val conn = getConnection(conf)
        val stmt = conn.createStatement()
        val query = s"SELECT * from `${conf.database}`.`${conf.tableName}` where 1 < 0"
        val rs = stmt.executeQuery(query)
        val rsmd = rs.getMetaData
        val validFieldTypes = mysqlToHiveTypeConverter.keys.toSet
        var validFields = Seq[DBField]()

        var setColumns = scala.collection.immutable.Set[String]()
        logger.info(s"************************ getColumnCount: ${rsmd.getColumnCount}")
        for (i <- 1 to rsmd.getColumnCount) {
            val columnType = rsmd.getColumnTypeName(i)

            if (validFieldTypes.contains(columnType.toUpperCase)) {
                val hiveColumnType = convertMySqlTypeToHiveType(columnType)
                val javaTypeMapping = {
                    if (hiveColumnType == "TIMESTAMP" || hiveColumnType == "DATE") Some("String")
                    else None
                }
                validFields = validFields :+ DBField(rsmd.getColumnName(i), hiveColumnType, javaTypeMapping)
            } else {
                validFields = validFields :+ DBField(rsmd.getColumnName(i), columnType, None)
            }
            setColumns = setColumns + rsmd.getColumnName(i).toLowerCase
            logger.info(s" column: ${rsmd.getColumnName(i)}, type: ${rsmd.getColumnTypeName(i)}," +
              s" precision: ${rsmd.getPrecision(i)}, scale:${rsmd.getScale(i)}\n")
        }
        rs.close()
        stmt.close()
        val primaryKey = getPrimaryKey(conn, setColumns, conf)
        conn.close()
        TableDetails(validFields, null, null, null, primaryKey)
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
                logger.warn(s"Could not access primary key $columnName")
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

    def convertMySqlTypeToHiveType(columnType: String): String = {
        var hiveType: String = mysqlToHiveTypeConverter(columnType.toUpperCase)
        if (hiveType == null)
            hiveType = columnType.toUpperCase

        hiveType
    }

    val mysqlToHiveTypeConverter: Map[String, String] = {
        Map(
            "TIME" -> "STRING",
            "DATETIME" -> "TIMESTAMP",
            "YEAR" -> "INT",
            "CHAR" -> "STRING",
            "VARCHAR" -> "STRING",
            "TINYBLOB" -> "BINARY",
            "BLOB" -> "BINARY",
            "MEDIUMBLOB" -> "BINARY",
            "LONGBLOB" -> "BINARY",
            "TINYTEXT" -> "STRING",
            "TEXT" -> "STRING",
            "JSON" -> "STRING",
            "MEDIUMTEXT" -> "STRING",
            "LONGTEXT" -> "STRING",
            "ENUM" -> "STRING",
            "SET" -> "STRING",
            "DECIMAL" -> "DECIMAL(38,10)",
            "TINYINT UNSIGNED" -> "SMALLINT",
            "SMALLINT UNSIGNED" -> "INT",
            "MEDIUMINT UNSIGNED" -> "INT",
            "INT UNSIGNED" -> "BIGINT",
            "BIGINT UNSIGNED" -> "BIGINT"
        )
    }

    override def transferDateFunction(sql: String): String = {null}

    override def transferCharFunction(sql: String): String = {null}

    override def createAndInsertExternalTable(conf: DBConfig): Unit = {}
}

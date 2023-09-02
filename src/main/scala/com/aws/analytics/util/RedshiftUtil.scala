package com.aws.analytics.util

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import com.aws.analytics.config.Configurations.DBConfiguration
import com.aws.analytics.config.InternalConfs.{DBField, InternalConfig, TableDetails}
import com.aws.analytics.config.RedshiftType
import com.aws.analytics.config.DBConfig
import org.slf4j.LoggerFactory

import scala.collection.immutable.{IndexedSeq, Set}


object RedshiftUtil {
    private val logger = LoggerFactory.getLogger(RedshiftUtil.getClass)


    def getJDBCUrl(conf: DBConfig): String = {
        val jdbcUrl = s"jdbc:${conf.database}://${conf.hostname}:${conf.portNo}/${conf.database}"
        if (conf.database.toLowerCase == "mysql")
            jdbcUrl + "?zeroDateTimeBehavior=convertToNull"
        else jdbcUrl
    }

    def getConnection(conf: DBConfig): Connection = {
        val connectionProps = new Properties()
        connectionProps.put("user", conf.userName)
        connectionProps.put("password", conf.password)
        val connectionString = getJDBCUrl(conf)
        Class.forName("com.amazon.redshift.jdbc4.Driver")
        DriverManager.getConnection(connectionString, connectionProps)
    }


    /**
      * Get all column names and data types from redshift
      *
      * @param dbConf redshift configuration
      * @return
      */
    def getColumnNamesAndTypes(dbConf: DBConfig): Map[String, String] = {
        logger.info("Getting all column names for {}", dbConf.toString)
        val query = s"SELECT * FROM ${dbConf.schema}.${dbConf.tableName} WHERE 1 < 0;"
        val connection = RedshiftUtil.getConnection(dbConf)
        val result: ResultSet = connection.createStatement().executeQuery(query)
        try {
            val resultSetMetaData = result.getMetaData
            val count = resultSetMetaData.getColumnCount

            val columnMap = (1 to count).foldLeft(Map[String, String]()) { (set, i) =>

                set + (resultSetMetaData.getColumnName(i).toLowerCase -> resultSetMetaData.getColumnTypeName(i))
            }
            columnMap
        } finally {
            result.close()
            connection.close()
        }
    }

    def getVacuumString(shallVacuumAfterLoad: Boolean, redshiftConf: DBConfig): String = {
        if (shallVacuumAfterLoad)
            s"VACUUM DELETE ONLY ${getTableNameWithSchema(redshiftConf)};"
        else ""
    }

    def performVacuum(conf: DBConfig): Unit = {
        logger.info("Initiating the connection for vacuum")
        val con = getConnection(conf)
        logger.info("Creating statement for Connection")
        val stmt = con.createStatement()
        val vacuumString = getVacuumString(shallVacuumAfterLoad = true, conf)
        logger.info("Running command {}", vacuumString)
        stmt.executeUpdate(vacuumString)
        stmt.close()
        con.close()
    }

    def getDropCommand(conf: DBConfiguration): String = {
        val tableNameWithSchema =
            if (conf.schema != null && conf.schema != "")
                s"${conf.schema}.${conf.tableName}"
            else
                conf.tableName
        s"DROP TABLE IF EXISTS $tableNameWithSchema;"
    }

    def createRedshiftTable(con: Connection, conf: DBConfiguration, createTableQuery: String, overwrite: Boolean = true): Unit = {
        val stmt = con.createStatement()
        if (overwrite) {
            stmt.executeUpdate(getDropCommand(conf))
        }
        stmt.executeUpdate(createTableQuery)
        stmt.close()
    }

    def getTableNameWithSchema(rc: DBConfig): String = {
        if (rc.schema != null && rc.schema != "") s"${rc.schema}.${rc.tableName}"
        else s"${rc.tableName}"
    }

    def getCreateTableString(td: TableDetails, conf: DBConfig): String = {
        val tableNameWithSchema = getTableNameWithSchema(conf)

        val fieldNames = td.validFields.map(r => s"""\t${r.fieldName.toLowerCase} ${r.fieldType} """).mkString(",\n")
        val distributionKey = td.distributionKey match {
            case None => "DISTSTYLE AUTO"
            case Some(key) => s"""DISTKEY ($key) """
        }
        val sortKeys = if (td.sortKeys.nonEmpty) "SORTKEY (" + td.sortKeys.mkString(", ") + ")" else ""

        val primaryKey = td.primaryKey match {
            case None => ""
            case Some(key) => s""",\n\tprimary key($key) """
        }

        s"""CREATE TABLE IF NOT EXISTS $tableNameWithSchema (
               |    $fieldNames $primaryKey
               |)
               |$distributionKey $sortKeys ;""".stripMargin

    }

}

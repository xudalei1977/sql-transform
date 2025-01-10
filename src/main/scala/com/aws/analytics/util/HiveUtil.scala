package com.aws.analytics.util

import com.aws.analytics.config.{DBConfig, InternalConfs}
import org.slf4j.{Logger, LoggerFactory}
import com.aws.analytics.config.InternalConfs.{DBField, InternalConfig, TableDetails}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.immutable.Seq


class HiveUtil() extends DBEngineUtil {
    private val logger: Logger = LoggerFactory.getLogger("HiveUtil")
    private val HIVE_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver"

    def getJDBCUrl(conf: DBConfig): String = {
        s"jdbc:hive2://${conf.hiveHost}:10000/${conf.database};"
    }

    def getConnection(conf: DBConfig): Connection = {
        val connectionString = getJDBCUrl(conf)
        println(s"connection string: = ${connectionString}" )
        Class.forName(HIVE_CLASS_NAME)
        DriverManager.getConnection(connectionString, "hadoop", "")
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

    def queryByJDBC(conn: Connection, sql: String) : Seq[String] = {
        var ps: PreparedStatement = null
        var rs: ResultSet = null
        var seq: Seq[String] = Seq()
        try {
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
        }
    }

    def msckRepairTable(conf: DBConfig): Unit = {
        val conn = getConnection(conf)
        val stmt = conn.createStatement
        stmt.execute(s"msck repair table ${conf.tableName}")
        stmt.close()
    }

    def getCreateTableString(td: TableDetails, conf: DBConfig): (String, Option[String]) = {

        /** mysql could not contain partition key, it could only specify a column as partition key, e.g. last_update_time */
        val partitonPt : DBField = DBField("last_update_time", "TIMESTAMP", None)

        var partitionKeys = scala.collection.immutable.Set[String]()
        var partitionStr = ""

        if (td.validFields.contains(partitonPt)) {
            partitionKeys = partitionKeys + "pt"
            partitionStr = "PARTITIONED BY (pt STRING)"
        } else {
            partitionStr = ""
        }

        val retPartitionKeys: Option[String] =
            if (partitionKeys.size == 1)  {
                Some(partitionKeys.toSeq.head)
            } else {
                None
            }

        val fieldsInCreateTable = td.validFields.map(r => s"""\t${r.fieldName.toLowerCase} ${r.fieldType} """).mkString(",\n")

        val createTableScript =
            s"""CREATE EXTERNAL TABLE if not exists `${conf.hiveDatabase}`.`${conf.tableName}` (
               |    $fieldsInCreateTable
               |) ${partitionStr}
               | stored as parquet
               | location '${conf.hiveInS3Path}/${conf.hiveDatabase}/${conf.tableName}'
               | tblproperties('comment' = 'generated by script',
               | 'owner' = '',
               | 'parquet.compression' = 'SNAPPY')""".stripMargin
        logger.info(s"*********** create table script is $createTableScript")
        (createTableScript, retPartitionKeys)
    }

    override def getTableDetails(conf: DBConfig, internalConfig: InternalConfig)(implicit crashOnInvalidType: Boolean): TableDetails = {null}

    override def transferDateFunction(sql: String): String = {null}

    override def transferCharFunction(sql: String): String = {null}

    override def createAndInsertExternalTable(conf: DBConfig): Unit = {}

    def getCreateTableString(tableName: String, conf: DBConfig): String = {
        val conn = getConnection(conf)
        val seq = queryByJDBC(conn, s"show create table ${conf.hiveDatabase}.$tableName" )
        var createTableDDL = seq.mkString(" \n").replaceAll(conf.hiveInHDFSPath, conf.hiveInS3Path)
        conn.close()

        createTableDDL = createTableDDL.replaceAll("ROW FORMAT SERDE", "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'")
        createTableDDL = createTableDDL.replaceAll("STORED AS INPUTFORMAT", "STORED AS TEXTFILE")

        createTableDDL += s";\n msck repair table ${tableName};"

        createTableDDL
    }
}

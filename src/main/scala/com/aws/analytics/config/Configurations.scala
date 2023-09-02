package com.aws.analytics.config

import com.aws.analytics.config.InternalConfs.InternalConfig

private[analytics] sealed trait Configuration

private[analytics] object Configurations {

    /** Used to all DB, e.g. MySQL, PG, Redshift, etc. */
    case class DBConfiguration(database: String,
                               db: String,
                               schema: String,
                               tableName: String,
                               hostname: String,
                               portNo: Int,
                               userName: String,
                               password: String,
                               preLoadCmd:Option[String] = None,
                               postLoadCmd:Option[String] = None) extends Configuration {

        override def toString: String = {
            s"""{
               |   Database Type: $database,
               |   Database Name: $db,
               |   Table Name: $tableName,
               |   Schema: $schema
               |}""".stripMargin
        }
    }

    case class S3Config(s3Location: String,
                        accessKey: String,
                        secretKey: String) extends Configuration


    case class AppConfiguration(mysqlConf: DBConfiguration,
                                redshiftConf: DBConfiguration,
                                s3Conf: S3Config,
                                internalConfig: InternalConfig,
                                status: Option[Status] = None,
                                migrationTime: Option[Double]= None) {

        override def toString: String = {
            val mysqlString: String = "\tmysql-db : " + mysqlConf.db + "\n\tmysql-table : " + mysqlConf.tableName
            val redshiftString: String = "\tredshift-schema : " + redshiftConf.schema + "\n\tredshift-table : " +
                    redshiftConf.tableName
            "{\n" + mysqlString + "\n" + redshiftString + "\n}"
        }
    }

}
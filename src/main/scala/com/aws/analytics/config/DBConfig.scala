package com.aws.analytics.config

case class DBConfig(ddlDir: String = "",
                    externalTableDir: String = "",
                    sqlDir: String = "",
                    sourceDBEngine: String ="mysql",
                    targetDBEngine: String ="hive",
                    hostname: String ="",
                    portNo: Int = 3306,
                    database: String = "",
                    schema: String = "",
                    tableName: String = "",
                    userName: String = "",
                    password: String = "",
                    mcRegion: String = "",
                    ossEndpoint: String = "",
                    ossUrl: String = "",
                    ossFormat: String = "",
                    ossFilter: String = "1=1",
                    hiveHost:String = "",
                    hiveDatabase:String = "",
                    hivePartitionValue:String = "",
                    hiveInS3Path: String = ""
                   )

object DBConfig {

  def parseConfig(obj: Object,args: Array[String]): DBConfig = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[DBConfig]("sql transform for " + programName) {
      head(programName, "1.0")
      opt[String]('g', "sourceDBEngine").optional().action((x, config) => config.copy(sourceDBEngine = x)).text("source database engine, e.g. adb_mysql, adb_pg")
      opt[String]('f', "ddlDir").optional().action((x, config) => config.copy(ddlDir = x)).text("file name to contain the create table sql")
      opt[String]('F', "externalTableDir").optional().action((x, config) => config.copy(externalTableDir = x)).text("file name to contain the create table sql")
      opt[String]('h', "hostname").optional().action((x, config) => config.copy(hostname = x)).text("source database hostname")
      opt[Int]('p', "portNo").optional().action((x, config) => config.copy(portNo = x)).text("source database port no.")
      opt[String]('d', "database").required().action((x, config) => config.copy(database = x)).text("source database name")
      opt[String]('s', "schema").optional().action((x, config) => config.copy(schema = x)).text("schema in source database")
      opt[String]('u', "userName").optional().action((x, config) => config.copy(userName = x)).text("user name to login source database")
      opt[String]('w', "password").optional().action((x, config) => config.copy(password = x)).text("password to login source database")
      opt[String]('r', "mcRegion").optional().action((x, config) => config.copy(mcRegion = x)).text("region of the maxcompute")
      opt[String]('o', "hiveInS3Path").optional().action((x, config) => config.copy(hiveInS3Path = x)).text("target table S3 location")
      opt[String]('e', "ossFilter").optional().action((x, config) => config.copy(ossFilter = x)).text("oss filter")

      programName match {
         case "ADBMySQL2Redshift" =>
          opt[String]('e', "ossEndpoint").optional().action((x, config) => config.copy(ossEndpoint = x)).text("oss endpoint")
          opt[String]('l', "ossUrl").optional().action((x, config) => config.copy(ossUrl = x)).text("oss url")
          opt[String]('f', "ossFormat").optional().action((x, config) => config.copy(ossFormat = x)).text("oss external table format")

        case "MaxCompute2Hive" =>
          opt[String]('l', "ossUrl").optional().action((x, config) => config.copy(ossUrl = x)).text("oss url")
          opt[String]('f', "ossFormat").optional().action((x, config) => config.copy(ossFormat = x)).text("oss external table format")

        case "MySQL2Hive" =>
          opt[String]('H', "hiveHost").optional().action((x, config) => config.copy(hiveHost = x)).text("hiveHost")
          opt[String]('D', "hiveDatabase").optional().action((x, config) => config.copy(hiveDatabase = x)).text("hiveDatabase")
          opt[String]('P', "hivePartitionValue").optional().action((x, config) => config.copy(hivePartitionValue = x)).text("hivePartitionValue")
          opt[String]('S', "hiveInS3Path").optional().action((x, config) => config.copy(hiveInS3Path = x)).text("hive table path")

         case "Hologres2Redshift" =>
      }
    }

    parser.parse(args, DBConfig()) match {
      case Some(conf) => conf
      case None => {
        System.exit(-1)
        null
      }
    }
  }
}

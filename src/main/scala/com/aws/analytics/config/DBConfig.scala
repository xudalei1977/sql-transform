package com.aws.analytics.config

case class DBConfig(fileName: String = "",
                    dbEngine: String ="mysql",
                    hostname: String ="",
                    portNo: Int = 3306,
                    region: String = "",
                    database: String = "",
                    schema: String = "",
                    tableName: String = "",
                    userName: String = "",
                    password: String = "",
                    directory: String = "",
                    ossEndpoint: String = "",
                    ossUrl: String = "",
                    accessID: String = "",
                    accessKey: String = "",
                    ossFormat: String = "",
                    ossFilter: String = "1=1",
                    s3Location: String = "",
                    exportDDL: Boolean = false
                   )

object DBConfig {

  def parseConfig(obj: Object,args: Array[String]): DBConfig = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[DBConfig]("sql transform for " + programName) {
      head(programName, "1.0")
      opt[String]('g', "dbEngine").required().action((x, config) => config.copy(dbEngine = x)).text("source database engine, e.g. adb_mysql, adb_pg")
      opt[String]('h', "hostname").optional().action((x, config) => config.copy(hostname = x)).text("source database hostname")
      opt[Int]('p', "portNo").optional().action((x, config) => config.copy(portNo = x)).text("source database port no.")
      opt[String]('d', "database").required().action((x, config) => config.copy(database = x)).text("source database name")
      opt[String]('s', "schema").optional().action((x, config) => config.copy(schema = x)).text("schema in source database")
      opt[String]('u', "userName").optional().action((x, config) => config.copy(userName = x)).text("user name to login source database")
      opt[String]('w', "password").optional().action((x, config) => config.copy(password = x)).text("password to login source database")
      opt[String]('r', "region").optional().action((x, config) => config.copy(region = x)).text("region of the source database")
      opt[String]('o', "s3Location").optional().action((x, config) => config.copy(s3Location = x)).text("target table S3 location")
      opt[Boolean]('e', "exportDDL").optional().action((x, config) => config.copy(exportDDL = x)).text("Export DDL from source table instead of do transform")
      opt[String]('i', "accessID").optional().action((x, config) => config.copy(accessID = x)).text("aliyun accessID")
      opt[String]('k', "accessKey").optional().action((x, config) => config.copy(accessKey = x)).text("aliyun accessKey")

      programName match {
        case "CreateTableSQL" =>
          opt[String]('f', "fileName").required().action((x, config) => config.copy(fileName = x)).text("file name to contain the create table sql")

        case "CommonSQL" =>
          opt[String]('r', "directory").optional().action((x, config) => config.copy(directory = x)).text("directory contain the sql to be transfered")

        case "ExternalTable" =>
          opt[String]('e', "ossEndpoint").optional().action((x, config) => config.copy(ossEndpoint = x)).text("oss endpoint")
          opt[String]('l', "ossUrl").optional().action((x, config) => config.copy(ossUrl = x)).text("oss url")
          opt[String]('f', "ossFormat").optional().action((x, config) => config.copy(ossFormat = x)).text("oss external table format")
          opt[String]('r', "ossFilter").optional().action((x, config) => config.copy(ossFilter = x)).text("oss filter")
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

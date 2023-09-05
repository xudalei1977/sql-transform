package com.aws.analytics.config

case class DBConfig(fileName: String = "",
                    dbEngine: String ="mysql",
                    hostname: String ="",
                    portNo: Int = 3306,
                    database: String = "",
                    schema: String = "",
                    tableName: String = "",
                    userName: String = "",
                    password: String = "",
                    directory: String = "")

object DBConfig {

  def parseConfig(obj: Object,args: Array[String]): DBConfig = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[DBConfig]("sql transform for " + programName) {
      head(programName, "1.0")
      opt[String]('g', "dbEngine").required().action((x, config) => config.copy(dbEngine = x)).text("source database engine, e.g. adb_mysql, adb_pg")

      programName match {
        case "CreateTableSQL" =>
          opt[String]('f', "fileName").required().action((x, config) => config.copy(fileName = x)).text("file name to contain the create table sql")
          opt[String]('h', "hostname").optional().action((x, config) => config.copy(hostname = x)).text("source database hostname")
          opt[Int]('p', "portNo").optional().action((x, config) => config.copy(portNo = x)).text("source database port no.")
          opt[String]('d', "database").required().action((x, config) => config.copy(database = x)).text("source database name")
          opt[String]('s', "schema").optional().action((x, config) => config.copy(schema = x)).text("schema in source database")
          opt[String]('u', "userName").optional().action((x, config) => config.copy(userName = x)).text("user name to login source database")
          opt[String]('w', "password").optional().action((x, config) => config.copy(password = x)).text("password to login source database")

        case "CommonSQL" =>
          opt[String]('r', "directory").optional().action((x, config) => config.copy(directory = x)).text("directory contain the sql to be transfered")
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



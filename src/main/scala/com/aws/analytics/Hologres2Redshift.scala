package com.aws.analytics

import com.aws.analytics.config.DBConfig
import com.aws.analytics.config.InternalConfs.InternalConfig
import com.aws.analytics.util.{ADBMySQLUtil, HologresUtil, _}
import org.slf4j.{Logger, LoggerFactory}

import java.io._

object Hologres2Redshift {

  private val logger: Logger = LoggerFactory.getLogger(Hologres2Redshift.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("start the create table sql script transform ...")
    val params = DBConfig.parseConfig(Hologres2Redshift, args)
    println(s"params := ${params.toString}")

    val ddlDir = new File(params.ddlDir + "/" + params.database)
    if (! ddlDir.exists())
      ddlDir.mkdirs()

    val hologresUtil = new HologresUtil()
    val redshiftUtil = new RedshiftUtil()

    val allTable: Seq[String] = if (params.tableName != "") Seq(params.tableName)
                                else hologresUtil.queryByJDBC(params, s"select tablename from pg_tables where schemaname = '${params.schema}'")

    /** create table sql */
    allTable.foreach(tableName => {
      val conf = DBConfig(hostname = params.hostname,
                                      portNo = params.portNo,
                                      userName = params.userName,
                                      password = params.password,
                                      database = params.database,
                                      schema = params.schema,
                                      tableName = tableName)

      val tableDetails = hologresUtil.getTableDetails(conf, internalConfig = InternalConfig())(false)
      val createTableString = redshiftUtil.getCreateTableString(tableDetails, conf)
      val file = new File(ddlDir.toString + "/" + tableName + ".sql")
      write2File(createTableString, file)
    })

    /** create external table and insert data, now the data is in OSS. */
    // hologresUtil.createAndInsertExternalTable(params)

    /** sql script transform from mysql to hive
     *  currently, only provide the Date function transform, will add more by regex, and test GenAI to transform. */
  }

  private def write2File(ddl: String, file: File): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))
    bw.write(ddl.toLowerCase())
    bw.write("\n")
    bw.close()
  }

}


//export ALI_CLOUD_ACCESS_KEY_ID=<Your AK>
//export ALI_CLOUD_ACCESS_KEY_SECRET=<Your SK>
//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar:./postgresql-42.3.2.jar
//scala com.aws.analytics.Hologres2Redshift \
//  -f /home/ec2-user/hologres \
//  -h hgpostcn-cn-omn3zsqor00a-cn-hangzhou.hologres.aliyuncs.com:80 \
//  -p 80 -d dev -s public -u $ALI_CLOUD_ACCESS_KEY_ID -w $ALI_CLOUD_ACCESS_KEY_SECRET

// PGUSER=$ALI_CLOUD_ACCESS_KEY_ID PGPASSWORD=$ALI_CLOUD_ACCESS_KEY_SECRET psql -p 80 -h hgpostcn-cn-omn3zsqor00a-cn-hangzhou.hologres.aliyuncs.com:80 -d dev
//for file in *.sql
//do
//  echo "Executing $file..."
//  hive -f "$file"
//done

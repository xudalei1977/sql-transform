package com.aws.analytics

import com.aws.analytics.config.DBConfig
import com.aws.analytics.config.InternalConfs.InternalConfig
import com.aws.analytics.util.{ADBMySQLUtil, _}
import org.slf4j.{Logger, LoggerFactory}

import java.io._

object ADBMySQL2Redshift {

  private val logger: Logger = LoggerFactory.getLogger(ADBMySQL2Redshift.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("start the create table sql script transform ...")
    val params = DBConfig.parseConfig(ADBMySQL2Redshift, args)
    println(s"params := ${params.toString}")

    val ddlDir = new File(params.ddlDir + "/" + params.database)
    if (! ddlDir.exists())
      ddlDir.mkdirs()

    val adbMySQLUtil = new ADBMySQLUtil()
    val redshiftUtil = new RedshiftUtil()

    val allTable: Seq[String] = if (params.tableName != "") Seq(params.tableName)
                                else adbMySQLUtil.queryByJDBC(params, "show tables") /** for pg: s"select tablename from pg_tables where schemaname = '${params.schema}'" */

    /** create table sql */
    allTable.foreach(tableName => {
      val conf = DBConfig(hostname = params.hostname,
                                      portNo = params.portNo,
                                      userName = params.userName,
                                      password = params.password,
                                      database = params.database,
                                      schema = params.schema,
                                      tableName = tableName)

      val tableDetails = adbMySQLUtil.getTableDetails(conf, internalConfig = InternalConfig())(false)
      val createTableString = redshiftUtil.getCreateTableString(tableDetails, conf)
      val file = new File(ddlDir.toString + "/" + tableName + ".sql")
      write2File(createTableString, file)
    })

    /** create external table and insert data, now the data is in OSS. */
    adbMySQLUtil.createAndInsertExternalTable(params)

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

//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.CreateTableSQL \
//  -f /home/ec2-user -g adb-mysql \
//  -h emr-workshop-mysql8.chl9yxs6uftz.us-east-1.rds.amazonaws.com \
//  -p 3306 -d dev -u admin -w Password****

//scala com.aws.analytics.CreateTableSQL \
//  -f /home/ec2-user -g adb-pg \
//  -h gp-bp1t4m428azo2zxk0o-master.gpdb.rds.aliyuncs.com \
//  -p 5432 -d adb_sampledata_tpch -s public -u postgres -w Password****

//export ALI_CLOUD_ACCESS_KEY_ID=<Your AK>
//export ALI_CLOUD_ACCESS_KEY_SECRET=<Your SK>
//export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
//scala com.aws.analytics.CreateTableSQL \
//  -f /home/ec2-user -g maxcompute \
//  -d mc_2_hive -r cn-hangzhou -o s3://dalei-demo/tmp

//for file in *.sql
//do
//  echo "Executing $file..."
//  hive -f "$file"
//done

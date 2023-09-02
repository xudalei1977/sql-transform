//package com.aws.analytics.common
//
//import java.sql.Types
//
//
//case object SqlShiftMySQLDialect extends JdbcDialect {
//
//    override def canHandle(url: String): Boolean = url.startsWith("jdbc:mysql")
//
//    override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
//        if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
//            // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
//            // byte arrays instead of longs.
//            md.putLong("binarylong", 1)
//            Option(LongType)
//        } else if (typeName.equals("TINYINT")) {
//            Option(IntegerType)
//        } else None
//    }
//
//    override def quoteIdentifier(colName: String): String = {
//        s"`$colName`"
//    }
//
//    override def getTableExistsQuery(table: String): String = {
//        s"SELECT 1 FROM $table LIMIT 1"
//    }
//
//    def registerDialect(): Unit = {
//        /* Removing pre registered mysql dialect to register new MySQL dialect which will not convert tinyint(1) to
//	 * boolean.
//	 */
//        val dialectsField = JdbcDialects.getClass.getDeclaredFields.filter(_.getName == "dialects")(0)
//        dialectsField.setAccessible(true)
//        val mySqlDialect =  dialectsField.get(JdbcDialects).asInstanceOf[List[JdbcDialect]].
//                filter(_.canHandle("jdbc:mysql")).head
//        JdbcDialects.unregisterDialect(mySqlDialect)
//        JdbcDialects.registerDialect(SqlShiftMySQLDialect)
//        /* Registration done */
//    }
//}
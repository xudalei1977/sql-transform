package com.aws.analytics.config

private[analytics] sealed trait InternalConf

private[analytics] object InternalConfs {

    case class DBField(fieldName: String,
                       fieldType: String,
                       javaType: Option[String] = None) extends InternalConf {

        override def toString: String = {
            s"""{
               |   Field Name: $fieldName,
               |   Field Type: $fieldType,
               |   Java Type: $javaType
               |}""".stripMargin
        }

        override def equals(obj: Any): Boolean = {
            obj match {
                case d: DBField => d.fieldName == fieldName
                case _ => false
            }
        }

        override def hashCode(): Int = {
            val prime = 31
            var result = 1
            result = prime * result + fieldName.hashCode()
            result
        }
    }

    case class TableDetails(validFields: Seq[DBField],
                            invalidFields: Seq[DBField],
                            sortKeys: Seq[String],
                            distributionKey: Option[String],
                            primaryKey: Option[String]) extends InternalConf {

        override def toString: String = {
            s"""{
               |   Valid Fields: $validFields,
               |   Invalid Fields: $invalidFields,
               |   Interleaved Sort Keys: $sortKeys,
               |   Distribution Keys: $distributionKey,
               |   Primary Keys: $primaryKey
               |}""".stripMargin
        }
    }

    //In the case of IncrementalSettings shallCreateTable should be false by default
    //whereCondition shall not be wrapped with brackets ()
    //Also whereCondition shall not be empty and shall be valid SQL

    //shallMerge: If false, new data will be appended, If true: It will be merged based on mergeKey
    //mergeKey: If mergeKey is not provided by default code uses primaryKey of the table as the mergeKey
    case class IncrementalSettings(whereCondition: String,
                                   shallMerge: Boolean = false,
                                   mergeKey: Option[String] = None,
                                   shallVacuumAfterLoad: Boolean = false,
                                   customSelectFromStaging: Option[String] = None,
                                   isAppendOnly: Boolean = false) extends InternalConf

    //Defaults,
    //If shallSplit = None then shallSplit = true

    //If shallOverwrite = None && incrementalSettings = None
    //    then shallOverwrite is true
    //If shallOverwrite = None && incrementalSettings != None
    //    then shallOverwrite is false
    //If shallOverwrite != None
    //    shallOverwrite = shallOverwrite.get

    //mapPartitions => set this with caution, If set to very high number, This can crash the database replica
    //reducePartitions => Parallelism is good for Redshift, Set this to >12, If this is same as the mapPartitions then
    //                      a reduce phase will be saved
    case class InternalConfig(shallSplit: Option[Boolean] = None,
                              distKey: Option[String] = None,
                              shallOverwrite: Option[Boolean] = None,
                              incrementalSettings: Option[IncrementalSettings] = None,
                              mapPartitions: Option[Int] = None,
                              reducePartitions: Option[Int] = None) extends InternalConf

}

case class RedshiftType(typeName: String,
                        hasPrecision: Boolean = false,
                        hasScale: Boolean = false,
                        precisionMultiplier: Int = 1){
    override def toString: String = {
        s"""{
           |   typeName: $typeName,
           |   hasPrecision: $hasPrecision,
           |   hasScale: $hasScale,
           |   precisionMultiplier: $precisionMultiplier
           |}""".stripMargin
    }

}
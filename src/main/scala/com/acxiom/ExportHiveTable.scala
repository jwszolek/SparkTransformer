package com.acxiom

import com.acxiom.HiveFromSpark.sc
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jwszol on 16/10/2017.
  */
object ExportHiveTable {

  val sparkConf = new SparkConf().setAppName("HiveFromSpark")
  val sc = new SparkContext(sparkConf)

  def exportToParquet {
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val getAllfromHiveTable = hiveContext.sql("select * from website.imdball")
    getAllfromHiveTable.write.format("parquet").save("/tmp/data_all.parquet")
  }
}

package com.acxiom

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, functions}

/**
  * Created by jwszol on 08/10/2017.
  */
object HiveFromSpark {

  val sparkConf = new SparkConf().setAppName("HiveFromSpark")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) {

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("CREATE TABLE IF NOT EXISTS website.imdbPrincFlat (tconst STRING, principalID STRING)")
    val getAllfromPrinc = hiveContext.sql("select * from website.imdbprincipal")
    val princArray = getAllfromPrinc.withColumn("princArray", functions.split(new Column("principalcast"),","))
    princArray.show()
    val princExplode = princArray.select(new Column("tconst"), functions.explode(new Column("princArray")).as("principalID"))
    princExplode.show()

    princExplode.write.mode("overwrite").saveAsTable("website.imdbPrincFlat")
  }

}

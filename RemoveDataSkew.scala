package com.gjeevanm.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RemoveDataSkewness extends App {
  val sparkconf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("RemoveDataSkewness")

  val spark = SparkSession
    .builder()
    .config(sparkconf)
    .getOrCreate()


  //we can set using sqlcontext
  // spark.sql("SET spark.sql.adaptive.enabled=true")

  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
  spark.conf.set("spark.sql.shuffle.partitions", "3")

  /*
    Lets create df1 with column ID which is having skew
   */

  import org.apache.spark.sql.functions._

  /*
   DataFrame (df1) has column ID
  */
  var df1 = spark.range(1000000000)
    .withColumn("id", lit("x"))

  val extravalues = spark.range(4)
    .withColumn("id", lit("y"))

  val moreextravalues = spark.range(4)
    .withColumn("id", lit("z"))

  df1 = df1.union(extravalues).union(moreextravalues)

  df.select("id").count(*).show(100, false)

  /*
  Lets create df2 with column ID
   */


  var df2 = spark.range(10000000)
    .withColumn("id", lit("x"))

  val morevalues = Seq(
    ("y"),
    ("z")).toDF

  df2 = df2.union(morevalues)

  /*
   Join two Df's on ID column
   */

  // lets enable AQE using below property- try running join with and without AQE enabled
  spark.conf.set("spark.sql.adaptive.enabled", "true")

  df1.join(
    df2,
    df1.col("id") <=> df2.col("id")
  ).select(df1("id")).groupBy(df1("id")).agg(count("*")).show(100, false)

}

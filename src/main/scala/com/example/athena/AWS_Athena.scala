package com.example.athena

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AWS_Athena extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
// Setting up Spark Conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","MyApp")
  sparkConf.set("spark.master", "local[2]")

//  Creating Spark Session
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

// Reading the orders data
  val ordersDF = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema", true) // Won't use in production
    .csv("/Users/Wolverine/Documents/BigData-Hadoop/Week 18/DataSets/students_with_header.csv")

  ordersDF.write
    .partitionBy("subject")
    .parquet("/Users/Wolverine/Documents/BigData-Hadoop/Week 18/DataSets/students_output_parquet")

  scala.io.StdIn.readLine()
  spark.stop()
}

package org.home

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

/**
  * Created by satish on 14/7/16.
  *
  * Conclusion ::
  * =============
  *
  * Number of Months in analysis: 199
  * Duration : 3rd Jan 2000 to 14th July 2016
  *
  * Lowset market close month wise :
  *
  * ===================
  *
  * First10Days : [81]
  * Mid10Days   : [45]
  * Last10Days  : [73]
  *
  * ===================
  *
  * Day (1-5)   : [58]
  * Day (6-10)  : [23]
  * Day (11-15) : [22]
  * Day (16-20) : [23]
  * Day (21-25) : [30]
  * Last5Days   : [43]
  *
  * ===================
  *
  * Day wise lowset market close (aggregated over month)
  *
  * 1  - 22
  * 2  - 10
  * 3  - 14
  * 4  - 5
  * 5  - 7
  * 6  - 2
  * 7  - 6
  * 8  - 7
  * 9  - 6
  * 10 - 2
  * 11 - 5
  * 12 - 5
  * 13 - 8
  * 14 - 4
  * 16 - 4
  * 17 - 5
  * 18 - 7
  * 19 - 2
  * 20 - 5
  * 21 - 6
  * 22 - 7
  * 23 - 5
  * 24 - 4
  * 25 - 8
  * 26 - 9
  * 27 - 7
  * 28 - 7
  * 29 - 4
  * 30 - 8
  * 31 - 8
  *
  * ===================
  **/
object BSEAnalysis {

  def main(args: Array[String]) {
    val sparkconf = new SparkConf().setAppName("BSE-Sensex").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkconf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sqlContext = new SQLContext(sparkContext)
    val inputFile = "sensex.csv";

    runBSEAnalysis(sqlContext, inputFile)
  }

  def runBSEAnalysis(sqlContext: SQLContext, inputFile: String): Unit = {
    val allData: DataFrame = prepareSensexData(sqlContext, inputFile)
    val tableName = "sensex";
    allData.registerTempTable(tableName)

    val minPerMonth = calculateDayWiseResults(sqlContext, tableName)
    minPerMonth.cache();
    printDayWiseResults(sqlContext, minPerMonth)

    val minPerYear = calculateMonthWiseResults(sqlContext, tableName)
    minPerYear.cache()
    printMonthWiseResults(sqlContext, minPerYear)
  }

  def prepareSensexData(sqlContext: SQLContext, inputFile: String): DataFrame = {
    val linesDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputFile)

    val getMonthYr: (String => String) = (date: String) => {
      date.split("-")(1) + date.split("-")(2)
    }

    val getMonth: (String => String) = (date: String) => {
      date.split("-")(1)
    }

    val getMonthNum: (String => Int) = (date: String) => {
      date.split("-")(1) match {
        case "January" => 1
        case "February" =>  2
        case "March"  => 3
        case "April"  =>  4
        case "May"  =>  5
        case "June" =>  6
        case "July" =>  7
        case "August" =>  8
        case "September"  =>  9
        case "October"  =>  10
        case "November" =>  11
        case "December" =>  12
      }
    }

    val getDay: (String => String) = (date: String) => {
      date.split("-")(0)
    }

    val getYear: (String => String) = (date: String) => {
      date.split("-")(2)
    }

    val getMonthFn = udf(getMonth)
    val getMonthYrFn = udf(getMonthYr)
    val getMonthNumFn = udf(getMonthNum)
    val getDayFn = udf(getDay)
    val getYearFn = udf(getYear)

    linesDF
      .withColumn("Day", getDayFn(linesDF("Date")))
      .withColumn("Month", getMonthFn(linesDF("Date")))
      .withColumn("MonthYr", getMonthYrFn(linesDF("Date")))
      .withColumn("MonthNum", getMonthNumFn(linesDF("Date")))
      .withColumn("Year", getYearFn(linesDF("Date")))
  }

  def calculateDayWiseResults(sqlContext: SQLContext, tableName: String): DataFrame = {
    sqlContext.sql("select MonthYr, min(Close) as mclose from " + tableName + " group by MonthYr")
      .registerTempTable("minclosepermonth")
    sqlContext.sql("select s.* from " + tableName + " s INNER JOIN " +
      "minclosepermonth m ON s.MonthYr = m.MonthYr AND s.Close = m.mclose")
  }

  def printDayWiseResults(sqlContext: SQLContext, minPerMonth: DataFrame): Unit = {
    minPerMonth.registerTempTable("minCloseMonth")
    println("=====================================")
    println("     Number of records: " + minPerMonth.count())
    println("---------------------------------")
    println("Day wise lowest closing :: ")
    sqlContext.sql("select Day, count(*) from minCloseMonth group by Day")
      .collect().foreach(rec => println(rec.mkString(" - ")))
    println("---------------------------------")
    println("5 day wise lowest closing :: ")
    sqlContext.sql("select count(*) from minCloseMonth where Day < 6")
      .collect().foreach(rec => println("Day (01-05) : " + rec))
    sqlContext.sql("select count(*) from minCloseMonth where Day > 5 And Day < 11")
      .collect().foreach(rec => println("Day (06-10) : " + rec))
    sqlContext.sql("select count(*) from minCloseMonth where Day > 10 And Day < 16")
      .collect().foreach(rec => println("Day (11-15) : " + rec))
    sqlContext.sql("select count(*) from minCloseMonth where Day > 15 And Day < 21")
      .collect().foreach(rec => println("Day (16-20) : " + rec))
    sqlContext.sql("select count(*) from minCloseMonth where Day > 20 And Day < 26")
      .collect().foreach(rec => println("Day (21-25) : " + rec))
    sqlContext.sql("select count(*) from minCloseMonth where Day > 25")
      .collect().foreach(rec => println("Last 5 Days : " + rec))
    println("---------------------------------")
    println("10 day wise lowest closing :: ")
    sqlContext.sql("select count(*) from minCloseMonth where Day < 11")
      .collect().foreach(rec => println("Day (01-10)  : " + rec))
    sqlContext.sql("select count(*) from minCloseMonth where Day > 10 And Day < 21")
      .collect().foreach(rec => println("Day (11-20)  : " + rec))
    sqlContext.sql("select count(*) from minCloseMonth where Day > 20")
      .collect().foreach(rec => println("Last 10 Days : " + rec))
    println("====================================")
  }

  def calculateMonthWiseResults(sqlContext: SQLContext, tableName: String): DataFrame = {
    sqlContext.sql("select Year, min(Close) as yclose from " + tableName + " group by Year")
      .registerTempTable("mincloseperyear")
    sqlContext.sql("select s.* from " + tableName + " s INNER JOIN " +
      "mincloseperyear y ON s.Year = y.Year AND s.Close = y.yclose")
  }

  def printMonthWiseResults(sqlContext: SQLContext, minPerYear: DataFrame): Unit = {
    minPerYear.registerTempTable("minCloseYear")
    println("=====================================")
    println("     Number of records: " + minPerYear.count())
    println("---------------------------------")
    println("Month wise lowest closing :: ")
    sqlContext.sql("select Month, count(*) from minCloseYear group by Month")
      .collect().foreach(rec => println(rec.mkString(" - ")))
    println("---------------------------------")
    println("Quater wise lowest closing :: ")
    sqlContext.sql("select count(*) from minCloseYear where MonthNum < 4")
      .collect().foreach(rec => println("Month (01-03) : " + rec))
    sqlContext.sql("select count(*) from minCloseYear where MonthNum > 3 And MonthNum < 7")
      .collect().foreach(rec => println("Month (04-06) : " + rec))
    sqlContext.sql("select count(*) from minCloseYear where MonthNum > 6 And MonthNum < 10")
      .collect().foreach(rec => println("Month (07-09) : " + rec))
    sqlContext.sql("select count(*) from minCloseYear where MonthNum > 9")
      .collect().foreach(rec => println("Month (10-12) : " + rec))
    println("---------------------------------")
    println("Half yearly lowest closing :: ")
    sqlContext.sql("select count(*) from minCloseYear where MonthNum < 7")
      .collect().foreach(rec => println("Month (01-06) : " + rec))
    sqlContext.sql("select count(*) from minCloseYear where MonthNum > 6")
      .collect().foreach(rec => println("Month (07-12) : " + rec))
    println("====================================")
  }
}
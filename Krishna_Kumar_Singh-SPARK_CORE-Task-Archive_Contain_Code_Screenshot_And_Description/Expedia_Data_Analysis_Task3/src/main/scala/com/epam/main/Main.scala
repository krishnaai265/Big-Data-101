package com.epam.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql.functions._
/**  
 * Main.java - This file contain the logic of spark session creation, reading CSV file, Spark data analysis code.
 * This application will also ask user to enter their file location. Then it will process that file.
 * @author  Krishna Kumar Singh
 * @role Junior Software Engineer
 * @contact krishna_singh1@epam.com
 */
object Main {
  def main(args: Array[String]) {        
        val sparkSession = createSparkSession()       
        val file = getUserFileLocation()
        val fileData = readCSVFile(sparkSession, file)
        displayAndFilterData(fileData)
        stopSparkSession(sparkSession)
    }

    def createSparkSession(): SparkSession = {
        SparkSession.builder.master("local").appName("CSVToAvroConverter").getOrCreate()
    }
    
    def stopSparkSession(sparkSession: SparkSession){
        sparkSession.stop()
    }
    
    def getUserFileLocation(): String = {
        println("Please Enter the file location For eg:C:\\Users\\Krishna_Singh1\\eclipse-workspace\\ExcelToAvroConverter\\src\\main\\resources\\train.csv")
        readLine()
    }
    
    def displayAndFilterData(fileData: DataFrame) {
        val df = fileData.filter("srch_adults_cnt>=1 AND srch_children_cnt >= 1 AND is_booking==0").groupBy("hotel_continent", "hotel_country", "hotel_market").count()
        df.sort(desc("count")).show(3)
    }
    
    def readCSVFile(sparkSession: SparkSession, file: String): DataFrame = {
        println("Reading files Started")
        sparkSession.read.option("header", "true").option("inferSchema", "true").csv(file) 
    }
}
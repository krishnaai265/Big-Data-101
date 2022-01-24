package com.epam.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/**  
 * Main.java - This file contain the logic of spark session creation, writing data to Kafka Topic.
 * This application will also ask user to enter their topic name & CSV file Location. Then it will read data from csv file and save it to that topic.
 * @author  Krishna Kumar Singh
 * @role Junior Software Engineer
 * @contact krishna_singh1@epam.com
 */ 
object Main {
  def main(args: Array[String]) {        
        val sparkSession = createSparkSession()
        val fileSchema = setSchema()
        val file = getUserFileLocation()
        val fileData = readCSVFile(sparkSession, file, fileSchema) 
        writeCSVFileDataOnTopic(fileData)
        stopSparkSession(sparkSession)        
    }
  
    def createSparkSession(): SparkSession = {
        SparkSession.builder.master("local").appName("GetKafkaTopicData").getOrCreate()
    }
    
    def stopSparkSession(sparkSession: SparkSession){
        sparkSession.stop()
    }
    
    def getUserFileLocation(): String = {
        println("Please Enter the file location For eg:C:\\Users\\Krishna_Singh1\\eclipse-workspace\\ExcelToAvroConverter\\src\\main\\resources\\train.csv")
        readLine()
    }
    
    def setSchema(): StructType = {
        StructType(Array(
        		StructField("date_time", StringType), 
        		StructField("site_name", IntegerType), 
        		StructField("posa_continent", IntegerType), 
        		StructField("user_location_country", IntegerType), 
        		StructField("user_location_region", IntegerType), 
        		StructField("user_location_city", IntegerType), 
        		StructField("orig_destination_distance", DoubleType), 
        		StructField("user_id", IntegerType), 
        		StructField("is_mobile", IntegerType), 
        		StructField("is_package", IntegerType), 
        		StructField("channel", IntegerType), 
        		StructField("srch_ci", StringType), 
        		StructField("srch_co", StringType), 
        		StructField("srch_adults_cnt", IntegerType), 
        		StructField("srch_children_cnt", IntegerType), 
        		StructField("srch_rm_cnt", IntegerType), 
        		StructField("srch_destination_id", IntegerType), 
        		StructField("srch_destination_type_id", IntegerType), 
        		StructField("is_booking", IntegerType), 
        		StructField("cnt", IntegerType), 
        		StructField("hotel_continent", IntegerType), 
        		StructField("hotel_country", IntegerType), 
        		StructField("hotel_market", IntegerType),  
        		StructField("hotel_cluster", IntegerType)
    		))
    }
    
    def readCSVFile(sparkSession: SparkSession, file: String, fileSchema: StructType): DataFrame = {
        println("Reading files Started")
    		sparkSession.readStream.schema(fileSchema).option("header", "true").option("inferSchema", "true").csv(file).filter("is_booking == 1")
   	}
    
    def writeCSVFileDataOnTopic(fileData: DataFrame){
        println("Please Enter topic data where you wants to upload data")
        var topicName=readLine()
        fileData.selectExpr("to_json(struct(*)) AS value").writeStream.format("kafka").option("topic", topicName).option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "/home/krishna/Downloads/kafka_2.12-2.0.0/lbc").start()
    }
  
}
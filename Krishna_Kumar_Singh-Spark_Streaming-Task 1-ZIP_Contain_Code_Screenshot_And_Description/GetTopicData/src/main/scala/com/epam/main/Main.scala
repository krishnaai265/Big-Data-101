package com.epam.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import sys.process._
/**  
 * Main.java - This file contain the logic of spark session creation, reading Kafka Topic Data file & writing back data to the hdfs.
 * This application will also ask user to enter their topic name. Then it will start getting data from topic.
 * @author  Krishna Kumar Singh
 * @role Junior Software Engineer
 * @contact krishna_singh1@epam.com
 */ 
object Main {
    def main(args: Array[String]):Unit= {         
       val sparkSession = createSparkSession() 
        val fileSchema = setSchema()
        val df = readKafkaTopicData(sparkSession, fileSchema)
        writeTopicDataLocally(df, fileSchema)
        writeTopicDataHdfs(sparkSession)
        stopSparkSession(sparkSession)
    }
  
    def createSparkSession(): SparkSession = {
        SparkSession.builder.master("local").appName("GetKafkaTopicData").getOrCreate()
    }
    
    def stopSparkSession(sparkSession: SparkSession){
        sparkSession.stop()
    }
    
    def writeTopicDataLocally(df: DataFrame, mySchema: StructType){
        df.write.option("header", "true").option("inferSchema", "true").csv("/home/krishna/Downloads/kafka_2.12-2.0.0/abc")        
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
    
    def readKafkaTopicData(sparkSession: SparkSession, fileSchema: StructType):DataFrame = {
        println("please enter topic name")
        val topicName = getUserFileLocation();
        val dfa = sparkSession.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", topicName).option("startingOffsets", "earliest").load()
        dfa.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").select(col("key").cast("string"), from_json(col("value").cast("string"), fileSchema).as("data")).select("data.*")
    }
    
    def writeTopicDataHdfs(sparkSession: SparkSession){
        var abc = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("/home/krishna/Downloads/kafka_2.12-2.0.0/abc/part-00000-74b59429-aec2-4e44-bf7c-f25ef5293fb4-c000.csv")
        var result = "hdfs dfs -put /home/krishna/Downloads/kafka_2.12-2.0.0/abc/part-00000-74b59429-aec2-4e44-bf7c-f25ef5293fb4-c000.csv /expedia-booking-data/train.csv"!  ;
    }  
}
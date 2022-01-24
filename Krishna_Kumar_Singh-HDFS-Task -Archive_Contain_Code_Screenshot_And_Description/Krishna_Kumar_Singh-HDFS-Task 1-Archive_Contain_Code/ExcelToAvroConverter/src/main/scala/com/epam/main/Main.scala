package com.epam.main

import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql._
/**  
 * Main.java - This file contain the logic of spark session creation, reading CSV file, Writing Avro file.
 * This application will also ask user to enter their file location. Then it will process that location.
 * @author  Krishna Kumar Singh
 * @role Junior Software Engineer
 * @contact krishna_singh1@epam.com
 */ 
object Main {
    def main(args: Array[String]) {        
        val sparkSession = createSparkSession()
        val files = getUserFilesLocation()
        val filesDirectories = getListOfFiles(files) 
        readCSVAndWriteAvroFiles(filesDirectories, sparkSession) 
        stopSparkSession(sparkSession)
    }

    def createSparkSession(): SparkSession = {
        SparkSession.builder.master("local").appName("CSVToAvroConverter").getOrCreate()
    }
    
    def stopSparkSession(sparkSession: SparkSession){
        sparkSession.stop()
    }
    
    def getUserFilesLocation(): String = {
        println("Please Enter the file location For eg:C:\\Users\\Krishna_Singh1\\eclipse-workspace\\ExcelToAvroConverter\\src\\main\\resources\\")
        readLine()
    }
 
    def readCSVAndWriteAvroFiles(filesDirectory: List[String], sparkSession: SparkSession){
        filesDirectory.foreach {
            file =>
              println("Operation Start on " + file)
              val csvData = readCSVFile(sparkSession, file)
              println(csvData.getClass.toString())
              writeAvroFile(csvData, file)
         }
    }
    
    def readCSVFile(sparkSession: SparkSession, file: String): DataFrame = {
        println("Reading files Started")
        sparkSession.read.option("header", "true").option("inferSchema", "true").csv(file) 
    }
    
    def writeAvroFile(csvData:DataFrame, file: String) {
        println("Writing files Started")
        csvData.write.format("avro").save("src\\main\\outputfiles\\"+getFileNameByRemovingFileExtenstion(file))
    }
    
    def getFileNameByRemovingFileExtenstion(file: String): String = {
        val fileName = file.split("\\\\").takeRight(1).toList
        fileName.head.dropRight(4)
    }   
    
    def getListOfFiles(baseDirectory: String): List[String] = {
        println("Files Names reading Started")
        val fileDirectory = new File(baseDirectory)
        fileDirectory.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv")).map(_.getPath).toList
     }
}
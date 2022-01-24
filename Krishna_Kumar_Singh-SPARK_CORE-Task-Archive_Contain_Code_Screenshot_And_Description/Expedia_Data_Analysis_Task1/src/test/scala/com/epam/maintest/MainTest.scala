package com.epam.maintest

import com.epam.main.Main
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.apache.spark.sql._
/**  
 * MainTest.java - This file test the Session creation functionality. And also verify schema. 
 * @author  Krishna Kumar Singh
 * @role Junior Software Engineer
 * @contact krishna_singh1@epam.com
 */
class MainTest {
    val sparkSession = Main.createSparkSession();
    @Test
    def createSparkSessionTest() {
    		var appName = sparkSession.sparkContext.getConf.getOption("spark.app.name").toString()
        var extractedAppName = appName.substring(appName.indexOf("(")+1, appName.indexOf(")"))
    		assertEquals("CSVDataAnalysis", extractedAppName);
	  }
    
    @Test
    def readCSVFileTest() {
        var userFileHeader = sparkSession.read.csv("C:\\Users\\Krishna_Singh1\\eclipse-workspace\\ExcelToAvroConverter\\ExcelToAvroConverter\\src\\main\\resources\\train.csv").first()
        var headerForValidation = Row("date_time", "site_name", "posa_continent", "user_location_country", "user_location_region", "user_location_city", "orig_destination_distance", "user_id", "is_mobile", "is_package", "channel", "srch_ci", "srch_co", "srch_adults_cnt", "srch_children_cnt", "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "is_booking", "cnt", "hotel_continent", "hotel_country", "hotel_market",  "hotel_cluster")
        assertEquals(headerForValidation, userFileHeader)
    }
}
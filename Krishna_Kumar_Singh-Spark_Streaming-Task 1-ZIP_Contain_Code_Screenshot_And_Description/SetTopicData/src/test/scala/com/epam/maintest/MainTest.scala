package com.epam.maintest

import com.epam.main.Main
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.apache.spark.sql._
/**  
 * MainTest.java - This file test the Session creation functionality.  
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
    		assertEquals("GetKafkaTopicData", extractedAppName);
	  }
}
package com.spark.learning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLExampleCreateTable {

	public static void main(String[] args) {
		SparkSession session=SparkSession.builder().appName("parkSQLExample").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
		//Logger.getLogger("");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		Dataset<Row> dataSet=session.read().option("header", true).csv("src/main/resources/students.csv");
		
		dataSet.createOrReplaceTempView("students_table");
		
		Dataset<Row> resultStudent=session.sql("select subject,year from students_table where year='2006'");
		resultStudent.show();
		
		
		
		
		
		
		//System.out.println(dataSet.count());
		
		//Get First Elements in DataSets
		
		
		
		session.close();
		

	}

}

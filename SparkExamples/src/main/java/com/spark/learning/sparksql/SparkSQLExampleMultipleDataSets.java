package com.spark.learning.sparksql;

import org.apache.log4j.Level;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkSQLExampleMultipleDataSets {

	public static void main(String[] args) {
		SparkSession session=SparkSession.builder().appName("parkSQLExample").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
		//Logger.getLogger("");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		Dataset<Row> dataSet=session.read().option("header", true).csv("src/main/resources/students.csv");
		
		dataSet.createOrReplaceTempView("students_table");
		
		Dataset<Row> resultStudent=session.sql("select subject,year from students_table where year='2006'");
		resultStudent.show();
		
		resultStudent.createOrReplaceTempView("smallerTable");
		
		Dataset<Row> smallerDataSet=session.sql("select subject,year from smallerTable ");
		smallerDataSet.show();
		
		
		//Another Way Of Querying Data through Java API way
		System.out.println("Data Set through Java API");
		Dataset<Row> dataSetOfSubj=	dataSet.select(col("subject").alias("SubjectOf"),col("year").alias("YEAR Of"));
		
		dataSetOfSubj=dataSetOfSubj.orderBy(col("subject"),col("year"));
		
		dataSetOfSubj.show(4);
		
		
		
		//System.out.println(dataSet.count());
		
		//Get First Elements in DataSets
		
		
		
		session.close();
		

	}

}

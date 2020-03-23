package com.spark.learning.sparksql;

import org.apache.log4j.Level;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class SparkSQLUDFExample {

	public static void main(String[] args) {
		SparkSession session=SparkSession.builder().appName("parkSQLExample").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
		//Logger.getLogger("");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		Dataset<Row> dataSet=session.read().option("header", true).csv("src/main/resources/students.csv");
		
		//dataSet.show();
		dataSet=dataSet.withColumn("pass", functions.col("grade").equalTo("A+"));
		
		
		//Creating UDT
		
		session.udf().register("isPassed",(String str)-> str.equalsIgnoreCase("A+"),DataTypes.BooleanType);
		dataSet=dataSet.withColumn("IsPass", functions.callUDF("isPassed",col("grade")));
	
		dataSet.show();
		
		//System.out.println(dataSet.count());
		
		//Get First Elements in DataSets
		
		/*
		 * Row firstRow=dataSet.first(); //String value=firstRow.get(2).toString();
		 * String value=firstRow.getAs("subject");
		 * 
		 * System.out.println("value"+value);
		 */
		
		session.close();
		

	}

}

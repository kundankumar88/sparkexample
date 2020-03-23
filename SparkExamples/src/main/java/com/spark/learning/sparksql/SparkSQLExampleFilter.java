package com.spark.learning.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLExampleFilter {

	public static void main(String[] args) {
		SparkSession session=SparkSession.builder().appName("parkSQLExample").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
		//Logger.getLogger("");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		Dataset<Row> dataSet=session.read().option("header", true).csv("src/main/resources/students.csv");
		
		Dataset<Row> dataSet1=dataSet.filter("subject='Math'");
		dataSet1.show();
		
		//Filter Using Lambda
		
		Dataset<Row> filteredDataSet=dataSet.filter(row-> row.getAs("subject").equals("Modern Art"));
		
		System.out.println("Result of Filtered Data Set is ");
		filteredDataSet.show(5);
		
		//System.out.println(dataSet.count());
		
		//Get First Elements in DataSets
		
		Row firstRow=dataSet.first();
		//String value=firstRow.get(2).toString();
		String value=firstRow.getAs("subject");
		
		System.out.println("value"+value);
		
		
		session.close();
		

	}

}

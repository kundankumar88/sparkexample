package com.spark.learning.SparkExamples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class FlatMapsAndFilterExamples {

	public List<String> getListOfLoggerData() {
		List<String> listOfDays = new ArrayList<String>();
		listOfDays.add("ERROR This is Error");
		listOfDays.add("WARNING This is Warning");
		listOfDays.add("WARNING This is Error");
		listOfDays.add("WARNING This is Error");
		listOfDays.add("ERROR This is Error");
		listOfDays.add("ERROR This is Error");
		listOfDays.add("FATAL This is FATAL");
		listOfDays.add("FATAL This is Error");

		return listOfDays;

	}

	public void useFlatMap(JavaSparkContext sc) {

		List<String> listOfDays = getListOfLoggerData();
		JavaRDD<String> javaRDD = sc.parallelize(listOfDays);

		JavaRDD<String> allWordsAcrossLines = javaRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

		JavaRDD<Long> values = allWordsAcrossLines.map(a -> 1L);

		Long count = values.reduce((a, b) -> (a + b));

		System.out.println("count of all values " + count);

	}

	public void useFlatMapCountByKeys(JavaSparkContext sc) {

		List<String> listOfDays = getListOfLoggerData();
		JavaRDD<String> javaRDD = sc.parallelize(listOfDays);

		JavaRDD<String> allWordsAcrossLines = javaRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

		JavaPairRDD<String, Integer> keyValuePairs = allWordsAcrossLines
				.mapToPair(eachVal -> new Tuple2<String, Integer>(eachVal, 1));

		JavaPairRDD<String, Integer> count = keyValuePairs.reduceByKey((val1, val2) -> val1 + val2);
		System.out.println("count of all key values ");

		count.foreach(eachVal -> System.out.println(eachVal));

	}
	
	public void useFilters(JavaSparkContext sc)
	{
		//Filter All words having is 
		
		List<String> listOfDays = getListOfLoggerData();
		JavaRDD<String> javaRDD = sc.parallelize(listOfDays);

		JavaRDD<String> allWordsAcrossLines = javaRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

		JavaPairRDD<String, Integer> keyValuePairs = allWordsAcrossLines.filter(eachWord->eachWord!=null && !eachWord.equalsIgnoreCase("This")).map(eachWord->eachWord.toUpperCase())
				.mapToPair(eachVal -> new Tuple2<String, Integer>(eachVal, 1));

		JavaPairRDD<String, Integer> count = keyValuePairs.reduceByKey((val1, val2) -> val1 + val2);
		System.out.println("count of all key values ");

		count.foreach(eachVal -> System.out.println(eachVal));
		
		
	}

	public static void main(String[] args) {
		FlatMapsAndFilterExamples example = new FlatMapsAndFilterExamples();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("SparkBasicExample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//example.useFlatMap(sc);
	//	example.useFlatMapCountByKeys(sc);
		example.useFilters(sc);
		sc.close();

	}

}

package com.spark.learning.SparkExamples;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class TuplesPairTuples {

	public List<Integer> getListOfData() {
		List<Integer> listOfDays = new ArrayList<Integer>();
		listOfDays.add(1);
		listOfDays.add(2);
		listOfDays.add(3);
		listOfDays.add(5);

		return listOfDays;

	}

	public List<String> getListOfLoggerData() {
		List<String> listOfDays = new ArrayList<String>();
		listOfDays.add("ERROR:This is Error");
		listOfDays.add("WARNING:This is Warning");
		listOfDays.add("WARNING:This is Error");
		listOfDays.add("WARNING:This is Error");
		listOfDays.add("ERROR:This is Error");
		listOfDays.add("ERROR:This is Error");
		listOfDays.add("FATAL:This is FATAL");
		listOfDays.add("FATAL:This is Error");

		return listOfDays;

	}

	public void displaySquareRoot(JavaSparkContext sc) {
		JavaRDD<Integer> rddForIntialData = sc.parallelize(getListOfData());

		JavaRDD<Tuple2<Integer, Double>> numberSquareRoot = rddForIntialData
				.map(a -> new Tuple2<Integer, Double>(a, Math.sqrt(a)));

		numberSquareRoot.foreach(a -> System.out.println(a));

	}

	public void displaySquareRootPairRDD(JavaSparkContext sc) {
		System.out.println("Start displaySquareRootPairRDD");
		JavaRDD<String> rddForIntialData = sc.parallelize(getListOfLoggerData());
		JavaPairRDD<String, Integer> keyValuePair = rddForIntialData.mapToPair(str -> {
			String val[] = str.split(":");

			return new Tuple2<String, Integer>(val[0], 1);
		}

		);

		JavaPairRDD<String, Integer> values = keyValuePair.reduceByKey((a, b) -> a + b);

		values.foreach((value) -> System.out.println(value));

		// JavaPairRDD<String, Iterable<String>> groupingByKeyRDD =
		// keyValuePair.groupByKey();

		/*
		 * Map<String, Long> keyValPairMap=keyValuePair.countByKey();
		 * System.out.println(keyValPairMap);
		 */

		System.out.println("End displaySquareRootPairRDD");

	}

	public void displaySquareRootGroupByKey(JavaSparkContext sc) {
		System.out.println("Start displaySquareRootGroupByKey");
		JavaRDD<String> rddForIntialData = sc.parallelize(getListOfLoggerData());
		JavaPairRDD<String, Integer> keyValuePair = rddForIntialData.mapToPair(str -> {
			String val[] = str.split(":");

			return new Tuple2<String, Integer>(val[0], 1);
		}
		
		

		);
		
		JavaPairRDD<String, Iterable<Integer>> values = keyValuePair.groupByKey();
		
		
		JavaPairRDD <String,Integer> keyValuesRDD=values.
				mapToPair(eachTuple-> new Tuple2<String, Integer>(eachTuple._1, Lists.newArrayList(eachTuple._2).stream().mapToInt(each->each).sum()));
		
		keyValuesRDD.foreach((value)-> System.out.println(value));
		
		
		//JavaPairRDD<String, Iterable<String>> groupingByKeyRDD = keyValuePair.groupByKey();
		
		
		
		
		/*
		 * Map<String, Long> keyValPairMap=keyValuePair.countByKey();
		 * System.out.println(keyValPairMap);
		 */
		
		System.out.println("End displaySquareRootPairRDD");

		

	}

	public static void main(String[] args) {
		TuplesPairTuples example = new TuplesPairTuples();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("SparkBasicExample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// example.printValues(sc);
		// example.reducerExamples(sc);
		// example.mapperExample(sc);
		// example.countRdd(sc);

		// example.displaySquareRoot(sc);
		//example.displaySquareRootPairRDD(sc);
		example.displaySquareRootGroupByKey(sc);

		// displaySquareRootPairRDD

		sc.close();

	}

}

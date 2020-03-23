package com.spark.learning.SparkExamples;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class SparkBasicExample {
	public List<Integer> getListOfData() {
		List<Integer> listOfDays = new ArrayList<Integer>();
		listOfDays.add(1);
		listOfDays.add(2);
		listOfDays.add(3);
		listOfDays.add(5);

		return listOfDays;

	}

	public void countRdd(JavaSparkContext sc) {
		System.out.println("Start *********************CountRDD");
		List<Integer> listOfDays = getListOfData();
		JavaRDD<Integer> eachNumberRDD = sc.parallelize(listOfDays);
		System.out.println("Total Number" + eachNumberRDD.count());

		System.out.println("End *********************CountRDD");

	}

	public void countRddUsingMapReduce(JavaSparkContext sc) {
		System.out.println("Start *********************CountRDD");
		List<Integer> listOfDays = getListOfData();
		JavaRDD<Integer> eachNumberRDD = sc.parallelize(listOfDays);
		Integer results = eachNumberRDD.map((each) -> new Integer(1)).reduce((a, b) -> a + b);
		System.out.println("Total Number" + results);
		System.out.println("End *********************CountRDD");

	}

	public void mapperExample(JavaSparkContext sc) {
		System.out.println("Start *********************mapperExample");
		List<Integer> listOfDays = getListOfData();
		JavaRDD<Integer> interValueRDD = sc.parallelize(listOfDays);
		JavaRDD<Double> sqRtList = interValueRDD.map((a) -> Math.sqrt(a));
		List<Double> values = sqRtList.take(4);
		System.out.println(values);

		System.out.println("End  *********************mapperExample");

	}

	public void reducerExamples(JavaSparkContext sc) {
		System.out.println("Start *********************reducerExamples");

		List<Integer> listOfDays = getListOfData();
		JavaRDD<Integer> lisOfNumberRDD = sc.parallelize(listOfDays);
		Integer sumOfValues = lisOfNumberRDD.reduce((a, b) -> a + b);

		System.out.println("Sum Of Values" + sumOfValues);
		System.out.println("End *********************reducerExamples");

	}

	public void printValues(JavaSparkContext sc) {
		List<Integer> listOfDays = getListOfData();

		JavaRDD<Integer> rddVal = sc.parallelize(listOfDays);

		JavaRDD<Integer> doubledRDD = rddVal.map(eachElement -> eachElement * 2);
		doubledRDD.foreach(eacVal -> System.out.println("Val:" + eacVal));

	}

	public static void main(String[] args) {

		SparkBasicExample example = new SparkBasicExample();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("SparkBasicExample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// example.printValues(sc);
		// example.reducerExamples(sc);
		// example.mapperExample(sc);
		// example.countRdd(sc);

		example.countRddUsingMapReduce(sc);

		sc.close();

	}
}

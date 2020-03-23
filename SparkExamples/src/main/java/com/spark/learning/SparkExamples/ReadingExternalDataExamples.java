package com.spark.learning.SparkExamples;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ReadingExternalDataExamples {

	public static void main(String[] args) {
		// Word Ranking Example:

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("ReadingExternalDataExamples").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> fileRDD = sc.textFile("src\\main\\resources\\input.txt");
		JavaPairRDD<String, Long> allValuesRDD = fileRDD
				.flatMap(eachLine -> Arrays.asList(eachLine.split(" ")).iterator())
				.filter(eachVal -> eachVal.matches("^((?=[A-Za-z])(?![_\\-]).)*$"))

				.filter(eachVal -> (eachVal != null && eachVal.trim().length() > 0))
				.map(eachVal -> eachVal.toUpperCase()).mapToPair(eachVal -> new Tuple2<String, Long>(eachVal, 1L))
				.reduceByKey((val1, val2) -> val1 + val2);

		JavaPairRDD<Long, String> allKeyValueSorted = allValuesRDD
				.mapToPair(eachVal -> new Tuple2<Long, String>(eachVal._2, eachVal._1)).sortByKey(false);
		
		allKeyValueSorted=allKeyValueSorted.coalesce(1);
		
		allKeyValueSorted.foreach(eachVal->System.out.println(eachVal));
		

		List<Tuple2<Long, String>> allKeyValueSortedVal = allKeyValueSorted.take(10);

		//System.out.println(allKeyValueSortedVal);

	}

}

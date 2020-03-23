package com.spark.learning.joins;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class RDDJoins {

	public static void main(String[] args) {
		RDDJoins example = new RDDJoins();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("RDDJoins").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// example.createInnerJoins(sc);
		// example.createLeftOuterJoins(sc);
		// example.createRightOuterJoins(sc);
		example.createCartesianJoins(sc);
		
		/*
		 * Scanner scanner=new Scanner(System.in); scanner.nextLine();
		 */
		sc.close();

	}

	public List<Tuple2<Integer, Integer>> getUserIdToScroreRDD() {
		List<Tuple2<Integer, Integer>> userIdToScore = new ArrayList<Tuple2<Integer, Integer>>();
		Tuple2<Integer, Integer> tuple1 = new Tuple2<Integer, Integer>(1, 95);
		Tuple2<Integer, Integer> tuple2 = new Tuple2<Integer, Integer>(2, 65);
		Tuple2<Integer, Integer> tuple3 = new Tuple2<Integer, Integer>(3, 72);
		Tuple2<Integer, Integer> tuple4 = new Tuple2<Integer, Integer>(4, 67);
		userIdToScore.add(tuple1);
		userIdToScore.add(tuple2);
		userIdToScore.add(tuple3);
		userIdToScore.add(tuple4);

		return userIdToScore;

	}

	public List<Tuple2<Integer, String>> getUserIdToNameRDD() {
		List<Tuple2<Integer, String>> userIdToScore = new ArrayList<Tuple2<Integer, String>>();
		Tuple2<Integer, String> tuple1 = new Tuple2<Integer, String>(1, "Kundan");
		Tuple2<Integer, String> tuple2 = new Tuple2<Integer, String>(2, "Amit");
		Tuple2<Integer, String> tuple3 = new Tuple2<Integer, String>(3, "Rohan");
		Tuple2<Integer, String> tuple4 = new Tuple2<Integer, String>(4, "Rajeev");
		Tuple2<Integer, String> tuple5 = new Tuple2<Integer, String>(5, "Rohan");
		userIdToScore.add(tuple1);
		userIdToScore.add(tuple2);
		userIdToScore.add(tuple3);
		userIdToScore.add(tuple4);
		userIdToScore.add(tuple5);

		return userIdToScore;

	}

	private void createInnerJoins(JavaSparkContext sc) {
		RDDJoins innerRDD = new RDDJoins();
		JavaPairRDD<Integer, Integer> userIdToScoreRDD = sc.parallelizePairs(innerRDD.getUserIdToScroreRDD());
		JavaPairRDD<Integer, String> userIdToNameRDD = sc.parallelizePairs(innerRDD.getUserIdToNameRDD());

		// Inner Join
		JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = userIdToScoreRDD.join(userIdToNameRDD);

		joinedRDD.foreach(eachPairRDD -> System.out.println(eachPairRDD));

	}

	private void createLeftOuterJoins(JavaSparkContext sc) {
		RDDJoins innerRDD = new RDDJoins();
		JavaPairRDD<Integer, Integer> userIdToScoreRDD = sc.parallelizePairs(innerRDD.getUserIdToScroreRDD());
		JavaPairRDD<Integer, String> userIdToNameRDD = sc.parallelizePairs(innerRDD.getUserIdToNameRDD());

		// Inner Join
		JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> joinedRDD = userIdToNameRDD
				.leftOuterJoin(userIdToScoreRDD);

		joinedRDD.foreach(eachPairRDD -> System.out.println(eachPairRDD));

	}

	private void createRightOuterJoins(JavaSparkContext sc) {
		RDDJoins innerRDD = new RDDJoins();
		JavaPairRDD<Integer, Integer> userIdToScoreRDD = sc.parallelizePairs(innerRDD.getUserIdToScroreRDD());
		JavaPairRDD<Integer, String> userIdToNameRDD = sc.parallelizePairs(innerRDD.getUserIdToNameRDD());

		// Inner Join
		JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> joinedRDD = userIdToNameRDD
				.rightOuterJoin(userIdToScoreRDD);

		joinedRDD.foreach(eachPairRDD -> System.out.println(eachPairRDD));

	}

	private void createCartesianJoins(JavaSparkContext sc) {
		RDDJoins innerRDD = new RDDJoins();
		JavaPairRDD<Integer, Integer> userIdToScoreRDD = sc.parallelizePairs(innerRDD.getUserIdToScroreRDD());
		JavaPairRDD<Integer, String> userIdToNameRDD = sc.parallelizePairs(innerRDD.getUserIdToNameRDD());

		// Inner Join
		JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, Integer>> joinedRDD = userIdToNameRDD
				.cartesian(userIdToScoreRDD);

		joinedRDD.foreach(eachPairRDD -> System.out.println(eachPairRDD));

	}
}

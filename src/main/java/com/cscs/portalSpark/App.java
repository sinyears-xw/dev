package com.cscs.portalSpark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.List;

public class App {
    public static void main( String[] args ) {
        SparkConf conf = new SparkConf().setAppName("wei").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
    }
}
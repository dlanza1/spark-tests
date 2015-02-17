package ch.cern.spark.tests;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkTest");
//		conf.set("spark.io.compression.codec", "lz4");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> file = sc.textFile("hdfs://itrac925:8020/user/hive/warehouse/data_numeric_csv/part-m-00000", 8);
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
		  public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
		  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		counts.saveAsTextFile("hdfs://itrac925:8020/user/hdfs/spark-out");
	}
	
}
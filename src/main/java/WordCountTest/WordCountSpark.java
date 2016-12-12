package WordCountTest;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCountSpark {
	public static void main(String[] args) {
	//	String logFile = "hdfs://hadoopmaster:9000/user/hadoop/test-spark-apps/test-wordCount.txt";
		String logFile = args[0];
		SparkConf conf=new SparkConf().setAppName("Word Count Application");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> data = sc.textFile(logFile).cache();
		
		//JavaRDD<String> words=data.flatMap(s->Arrays.asList(s.split(" ")));
		JavaRDD<String> words=data.flatMap(new FlatMapFunction<String,String>(){
			public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
                }
            });
		//JavaPairRDD<String, Integer> pairs=words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
		//JavaPairRDD<String, Integer> counter=pairs.reduceByKey((a,b)->a+b);
		 JavaPairRDD<String, Integer> counter = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	            public Integer call(Integer a, Integer b) {
	                return a + b;
	            }
	        });
		//counter.saveAsTextFile("hdfs://hadoopmaster:9000/user/hadoop/test-spark-apps/wordCount-result.txt");
		 counter.saveAsTextFile(args[1]);
		

	}
}

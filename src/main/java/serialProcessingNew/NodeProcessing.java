package serialProcessingNew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class NodeProcessing {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf config = new SparkConf().setMaster("local[4]").set("spark.driver.maxResultSize", "3g")
				.setAppName("First test on tile parallel processing");		//SparkConf config = new SparkConf().setAppName("Img process per node"); //ONLY in cluster
		//configure spark to use Kryo serializer instead of the java serializer. 
		//All classes that should be serialized by kryo, are registered in MyRegitration class .
		config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		config.set("spark.kryo.registrator", "tileBased.MyRegistrator").set("spark.kryoserializer.buffer.max", "550");
		//conf.set("spark.kryo.registrationRequired", "true");  //brought the catastrophe
		JavaSparkContext sc = new JavaSparkContext(config);
		System.out.println("adada");
		JavaRDD<String> lines = sc.textFile("/home/ethanos/Desktop/BDEimages/asd.txt");
		JavaRDD<String> words =
		    lines.flatMap(line -> Arrays.asList(line.split(" ")));
		JavaPairRDD<String, Integer> counts =
		    words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
		         .reduceByKey((x, y) -> x + y);
		counts.saveAsTextFile("/home/ethanos/Desktop/BDEimages/asdd");

	}

}
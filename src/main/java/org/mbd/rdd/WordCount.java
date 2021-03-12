package org.mbd.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF) ;

        // Step 1: create a SparkConf object
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add numbers")
                .setMaster("local[4]") ;

        // Step 2: create a Java Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf) ;

        // Step 3. Read the file content
        JavaRDD<String> lines = sparkContext.textFile("data/quijote.txt");

        int numberOflines = (int)lines.count();
        System.out.println("Number of lines is:" + numberOflines);

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word,1));

        JavaPairRDD<String, Integer> groupedPairs = pairs.reduceByKey((integer, integer2) -> integer + integer2);

        List<Tuple2< String, Integer>> results = groupedPairs.collect();

        for (Tuple2<?,?> tuple: results){
            System.out.println(tuple._1() + ":"+tuple._2());
        }



        // Step 7: stop the spark context
        sparkContext.stop() ;
    }
}

package srinathsureshkumar.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;


import java.util.Set;
import java.util.HashSet;

public class MostPopularWords {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCountInFiles").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputPath = args[0]; 
        JavaRDD<String> filesRDD = sc.textFile(inputPath);

        JavaPairRDD<String, String> wordFileRDD = filesRDD
                .flatMapToPair(fileContent -> {
                    String[] words = fileContent.split("\\s+");  
                    Set<String> uniqueWords = new HashSet<>(Arrays.asList(words)); 
                    return uniqueWords.stream()
                            .map(word -> new Tuple2<>(word, "dummy"))  
                            .iterator();
                });

        JavaPairRDD<String, Integer> wordCountRDD = wordFileRDD
                .mapToPair(tuple -> new Tuple2<>(tuple._1, 1))  
                .reduceByKey((a, b) -> a + b); 

        List<Tuple2<String, Integer>> topWords = wordCountRDD
                .top(30, new Tuple2Comparator());

        for (Tuple2<String, Integer> word : topWords) {
            System.out.println(word._1() + ": " + word._2() + " files");
        }

    
        sc.close();
    }

    static class Tuple2Comparator implements java.util.Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t1._2.compareTo(t2._2);
        }
    }
}



package srinathsureshkumar.spark;
    
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Top30Words {

    private static void wordCount(String intputFile, String outputFile) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(intputFile);

        JavaPairRDD<Integer, String> counts = inputFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b).mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1())).sortByKey(false);
        sparkContext.parallelize(counts.take(30)).saveAsTextFile(outputFile);;
        // counts.saveAsTextFile(outputFile);
        sparkContext.close();
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        wordCount(args[0], args[1]);
    }
}
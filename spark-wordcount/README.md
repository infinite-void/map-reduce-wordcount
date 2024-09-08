# spark-wordcount 

Spark-wordcount is a spark based map-reduce implementation of word count given a text file.

## Installing spark
Download the binary archive of spark from apache-spark webstie and unzip into a folder. We can use the spark-submit script in the bin to run spark jobs directly. 

## Packaging the class in a jar file
The project is designed as a maven project. Please run the below command to package the jar file. 
```
maven clean package
```


## Submitting Spark jobs
```
 ./spark-submit --class srinathsureshkumar.spark.MostPopularWords --master local[4] --driver-memory 8g --conf "spark.eventLog.dir=file:/tmp/spark-events" --conf "spark.eventLog.enabled=true" /Users/srinathsureshkumar/Workspace/map-reduce/spark-wordcount/target/spark-wordcount-1.0-SNAPSHOT.jar /Users/srinathsureshkumar/Workspace/map-reduce/gutenberg_books_5000
```
## git 
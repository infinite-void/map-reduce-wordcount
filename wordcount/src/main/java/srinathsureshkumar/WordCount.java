package srinathsureshkumar;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
/* 
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{


    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }


  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();


    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
*/  


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
    conf.set("mapreduce.jobtracker.address", "localhost:54311");
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "localhost:8032");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);


    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", args[2]);
    job.setNumReduceTasks(Integer.parseInt(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


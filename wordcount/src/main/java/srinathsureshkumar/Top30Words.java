package srinathsureshkumar;

import java.io.IOException;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// https://stackoverflow.com/questions/67085393/map-reduce-for-top-n-items

public class Top30Words {
    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {
        private int n; 
        private TreeMap<Integer, String> word_list; 

        public void setup(Context context) {
            n = Integer.parseInt(context.getConfiguration().get("N"));  // get N
            word_list = new TreeMap<Integer, String>();
        }

        public void map(Object key, Text value, Context context) {
            String[] line = value.toString().split("\t"); 
            word_list.put(Integer.valueOf(line[1]), line[0]);
            if (word_list.size() > n)
                word_list.remove(word_list.firstKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : word_list.entrySet())
            {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static class TopNReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        private int n; 
        private TreeMap<Integer, String> word_list;

        public void setup(Context context) {
            n = Integer.parseInt(context.getConfiguration().get("N"));  
            word_list = new TreeMap<Integer, String>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int wordcount = 0;
            for(IntWritable value : values)
                wordcount = value.get();

            word_list.put(wordcount, key.toString());

            if (word_list.size() > n)
                word_list.remove(word_list.firstKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : word_list.entrySet())
            {
                context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tmpPath = new Path("/wordcount/tmp/" +new Random().nextInt());
        String minSplitSize = args[2];
        String maxSplitSize = args[3];
        int reducers = Integer.parseInt(args[4]);

        
        Configuration conf = new Configuration();
        conf.set("N", "30");
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        conf.set("mapreduce.jobtracker.address", "localhost:54311");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", maxSplitSize);


        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(reducers);


        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, tmpPath);
        job.waitForCompletion(true);

        Job topn_job = Job.getInstance(conf, "TopN");
        topn_job.setJarByClass(Top30Words.class);
        topn_job.setMapperClass(TopNMapper.class);
        topn_job.setReducerClass(TopNReducer.class);
        topn_job.setMapOutputKeyClass(Text.class);
        topn_job.setMapOutputValueClass(IntWritable.class);
        topn_job.setOutputKeyClass(IntWritable.class);
        topn_job.setOutputValueClass(Text.class);
        topn_job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(topn_job, tmpPath);
        FileOutputFormat.setOutputPath(topn_job,  outputPath);
        System.exit(topn_job.waitForCompletion(true) ? 0 : 1);
    }
}

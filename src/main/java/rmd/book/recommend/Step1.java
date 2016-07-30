package rmd.book.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import rmd.book.hdfs.HdfsDAO;

public class Step1 {

    public static class Step1_ToItemPreMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            if (tokens[0].startsWith("u")) return;
            int userID = Integer.parseInt(tokens[0]);
            String itemID = tokens[1];
            String pref = Float.toString(Float.parseFloat(tokens[3])+Float.parseFloat(tokens[4]));
            int booklistId = Integer.parseInt(tokens[5]);
            k.set(booklistId);
            v.set(itemID + ":" + pref);
            context.write(k, v);
        }
    }

    public static class Step1_ToUserVectorReducer extends  Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            for(Text value:values){
            	sb.append("\t" + value);
            }
            v.set(sb.toString().replaceFirst("\t", ""));
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    	System.out.println("**************************");
    	System.out.println("Step1 started");
    	System.out.println("**************************");
    	Job job = Recommend.config();

        String input = path.get("Step1Input");
        String output = path.get("Step1Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, job);
        hdfs.rmr(output);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("data"), input);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step1_ToItemPreMapper.class);
        job.setCombinerClass(Step1_ToUserVectorReducer.class);
        job.setReducerClass(Step1_ToUserVectorReducer.class);


        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}

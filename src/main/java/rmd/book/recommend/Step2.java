package rmd.book.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import rmd.book.hdfs.HdfsDAO;

public class Step2 {
    public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            for (int i = 1; i < tokens.length; i++) {
                String itemID = tokens[i].split(":")[0];
                for (int j = 1; j < tokens.length; j++) {
                	if(i==j)continue;
                	String[] splited = tokens[j].split(":");
                    String itemID2 = splited[0];
                    String pref = splited[1];
                    k.set(itemID + ":" + itemID2);
                    v.set(pref);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step2_UserVectorToConoccurrenceReducer extends  Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for(Text value:values) {
                sum += Float.parseFloat(value.toString());
            }
            result.set(Float.toString(sum));
            context.write(key, result);
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    	System.out.println("**************************");
    	System.out.println("Step2 started");
    	System.out.println("**************************");
        Job job = Recommend.config();

        String input = path.get("Step2Input"); 
        String output = path.get("Step2Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, job);
        hdfs.rmr(output);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
//        conf.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
        job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
        }
}

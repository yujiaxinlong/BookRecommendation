package rmd.book.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import rmd.book.hdfs.HdfsDAO;

public class Step5 {
	
	
	
	
	public static class ExchangeMapper  extends   Mapper<Object, Text, DoubleWritable, Text>{
		private Text bookId = new Text();
		private final static DoubleWritable score = new DoubleWritable(1);
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(value.toString());
			bookId.set(tokens[1]);
			score.set(Double.parseDouble(tokens[2]));
			context.write(score, bookId);
		}
	}
 public static class SortReducer extends  Reducer<DoubleWritable,Text,Text,DoubleWritable> {
   private Text result = new Text();

	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
        for(Text value:values) {
        	result.set(value.toString());
 	        context.write(result, key);
        }
	       
	    	
	}

 }

 public static class DescSort extends  WritableComparator{  
	  
     public DescSort() {  
         super(IntWritable.class,true);//注册排序组件  
    }  
     @Override  
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,  
            int arg4, int arg5) {  
        return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);//注意使用负号来完成降序  
    }  
       
     @Override  
    public int compare(Object a, Object b) {  
   
        return   -super.compare(a, b);//注意使用负号来完成降序  
    }  
      
}  
 
 public static void run(Map<String, String> path) throws Exception {
 	System.out.println("**************************");
 	System.out.println("Step5 started");
 	System.out.println("**************************");
 	Job job = Recommend.config();

    String input = path.get("Step5Input");
    String output = path.get("Step5Output");
    HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, job);
    hdfs.rmr(output);

    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(ExchangeMapper.class);
    job.setReducerClass(SortReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setSortComparatorClass(DescSort.class);
    job.setPartitionerClass(TotalOrderPartitioner.class);
    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.waitForCompletion(true);

 }
}
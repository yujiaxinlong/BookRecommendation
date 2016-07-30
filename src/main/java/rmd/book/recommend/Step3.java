package rmd.book.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import rmd.book.hdfs.HdfsDAO;

public class Step3 {

    public static class Step3_PartialMultiplyMapper extends Mapper<Object, Text, Text, Text> {

        private String flag;// A同现矩阵 or B评分矩阵

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集

            // System.out.println(flag);
        }

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            

            if (flag.equals("step2")) {// 同现矩阵
            	String[] tokens = Recommend.DELIMITER.split(values.toString());
                String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];

                Text k = new Text(itemID1);
                Text v = new Text("A:" + itemID2 + "\t" + num);

                context.write(k, v);
                // System.out.println(k.toString() + "  " + v.toString());

            } else if (flag.equals("booklist")) {// 评分矩阵
                String[] line = values.toString().split(":");
                String itemID = line[0];
                String pref = line[1];

                Text k = new Text(itemID);
                Text v = new Text("B:" + pref);

                context.write(k, v);
                // System.out.println(k.toString() + "  " + v.toString());
            }
        }

    }

    public static class Step3_AggregateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println(key.toString() + ":");

            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text line : values) {
                String val = line.toString();
                //System.out.println(val);

                if (val.startsWith("A:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);

                } else if (val.startsWith("B:")) {
                    String pref =val.substring(2);
                    mapB.put("0", pref);

                }
            }

            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();// itemID

                float num = Float.parseFloat(mapA.get(mapk));
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();// userID
                    double pref = Double.parseDouble(mapB.get(mapkb));
                    result = num * pref;// 矩阵乘法相乘计算
                    Text k = new Text(mapkb);
                    Text v = new Text(mapk + "\t" + result);
                    context.write(k, v);
                    //System.out.println(k.toString() + "  " + v.toString());
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	System.out.println("**************************");
    	System.out.println("Step3 started");
    	System.out.println("**************************");
        Job job = Recommend.config();
        String input1 = path.get("Step3Input1");
        String input2 = path.get("Step3Input2");
        String output = path.get("Step3Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, job);
        hdfs.mkdirs(input2);
        hdfs.copyFile(path.get("booklist"), input2);
        hdfs.rmr(output);

        job.setJarByClass(Step3.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step3.Step3_PartialMultiplyMapper.class);
        job.setReducerClass(Step3.Step3_AggregateReducer.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }

}
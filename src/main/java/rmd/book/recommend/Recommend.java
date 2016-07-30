package rmd.book.recommend;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.PropertyConfigurator;

import rmd.book.hdfs.HdfsDAO;

public class Recommend {

    public static final String HDFS = "hdfs://localhost:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t]");

    public static void main(String[] args) throws Exception {
    	
    	String log4jConfPath = "conf/log4j.properties";
    	PropertyConfigurator.configure(log4jConfPath);
    	
    	
        Map<String, String> path = new HashMap<String, String>();
        String[] booklist = {"32042","57","23490","19530","2225"};
        BufferedWriter writer = new BufferedWriter(new FileWriter("booklist.csv"));
        for(String book:booklist){
        	writer.write(book+":"+"5\n");
        }
		writer.flush();
		writer.close();
		path.put("booklist", "booklist.csv");
        path.put("data", "yousuu_items.csv");
        path.put("root",HDFS+"/user/yu/recommend");
        path.put("Step1Input", "/user/yu/recommend/books");
        path.put("Step1Output", path.get("root") + "/step1");
        
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("root") + "/step2");
        
        path.put("Step3Input1", path.get("Step2Output"));
        path.put("Step3Input2", "/user/yu/recommend/booklist");
        path.put("Step3Output", path.get("root") + "/step3");
        
        path.put("Step4Input", path.get("Step3Output"));
        path.put("Step4Output", path.get("root") + "/step4");
        
        path.put("Step5Input", path.get("Step4Output"));
        path.put("Step5Output", path.get("root") + "/step5");
//        Step1.run(path);
//        Step2.run(path);
//        Step3.run(path);
//        Step4.run(path);
//        Step5.run(path);
        Step6.run(path);
    }

    public static Job config() throws IOException {
    	Configuration conf = new Configuration();
    	conf.addResource(new Path("conf/core-site.xml"));
    	conf.addResource(new Path("conf/hdfs-site.xml"));
        Job job = Job.getInstance(conf,"recommand");
        //job.set.set("mapreduce.task.io.sort.mb", "1024");
        return job;
    }

}

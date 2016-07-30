package rmd.book.recommend;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;

import rmd.book.hdfs.HdfsDAO;

public class Step6 {
	
	public static void run(Map<String, String> path) throws IOException, SQLException{
        Job job = Recommend.config();
        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, job);
        String[] Top5 = hdfs.lines(path.get("Step5Output")+"/part-r-00000", 5);
        System.out.println(Top5[0]);

          //replace "hive" here with the name of the user the queries should run as
          Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000", "", "");
          Statement stmt = con.createStatement();
          ResultSet res = stmt.executeQuery("SELECT name FROM books WHERE bookid = '23'");
          if (res.next()) {
              System.out.println(res.getString(1));
            }
        System.exit(0);
	}
}

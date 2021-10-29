package ProductSales.ProductSales;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	Configuration config = new Configuration();
        
        Path inputPath = new Path("hdfs://127.0.0.1:9000/input/SalesJan2009.csv");
        Path outputPath = new Path("hdfs://127.0.0.1:9000/output/result.txt");
        
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), config);
        if(hdfs.exists(outputPath)) {
        	hdfs.delete(outputPath, true);
        }
        
        JobConf salesJob = new JobConf(config, App.class);
        
        salesJob.setJobName("Sales");
        salesJob.setJarByClass(App.class);
        salesJob.setOutputKeyClass(Text.class);
        salesJob.setMapOutputValueClass(IntWritable.class);
        salesJob.setOutputFormat(TextOutputFormat.class);
        salesJob.setMapperClass(SalesMapper.class);
        salesJob.setReducerClass(SalesReducer.class);
        
        FileInputFormat.setInputPaths(salesJob, inputPath);
        FileOutputFormat.setOutputPath(salesJob, outputPath);
        
        RunningJob result = JobClient.runJob(salesJob);
        
        System.out.print("Result=" + result.isComplete());
    }
}

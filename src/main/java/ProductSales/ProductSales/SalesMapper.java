package ProductSales.ProductSales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>  {
	
	String input;
	String[] separated;
	Text mappingKey = new Text();
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		if(key.get() == 0) return;
		
		input = value.toString();	
		separated = input.split(",");
		
		for(int i = 0; i<separated.length; i++) {
			separated[i] = separated[i].trim();
		}
		
		mappingKey.set("Payment type: " + separated[3]);
		output.collect(mappingKey, new IntWritable(1));
		
		mappingKey.set("Product type: " + separated[1]);
		output.collect(mappingKey, new IntWritable(1));
		
		mappingKey.set("Transaction date: " + getTransactionDay(separated[0]));
		output.collect(mappingKey, new IntWritable(1));
	}
	
	
	private String getTransactionDay(String date) {
		return date.split("\\/|\\.")[1];
	}

}

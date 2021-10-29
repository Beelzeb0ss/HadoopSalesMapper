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
	ArrayList<String> separated;
	Text mappingKey;
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		if(key.get() == 0) return;
		
		input = value.toString();	
		separated = (ArrayList<String>) Arrays.asList(input.split(","));
		
		
		mappingKey.set("Payment type: " + separated.get(3));
		output.collect(mappingKey, new IntWritable(1));
		
		mappingKey.set("Product type: " + separated.get(1));
		output.collect(mappingKey, new IntWritable(1));
		
		mappingKey.set("Transaction date: " + getTransactionDay(separated.get(0)));
		output.collect(mappingKey, new IntWritable(1));
	}
	
	
	private String getTransactionDay(String date) {	
		return date.split("/|.")[1];
	}

}

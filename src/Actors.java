
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Actors {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	 
	 
	public  ActorList actors = new ActorList();
	public  int actorSize = 0;
	

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
          	String line = value.toString();
          	String url = "";
          	
          	if(line.toLowerCase().startsWith("warc-target-uri:")) {
          		url = line.split(" ")[1];
          	}
   		   	
          	for(String actor : actors.getActors()){
          		if(line.contains(actor)){
          			actors.found(actor);
          			context.write(new Text(actor),new Text(url));
          		}
          	}
          	if(line.toLowerCase().equals("warc/1.0")){
          		actors.reset();
          	}       
            
    }
 } 
 
 public static class Reduce extends Reducer<Text, Text, Bigram, IntWritable> {

    public IntWritable one = new IntWritable(1);
    
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
                
    	ArrayList<String> cache = new ArrayList<String>();
    	
    	for(Text val:values) {
    		cache.add(val.toString());
    	}
    	
    	for(int i = 0; i < cache.size(); i++){
    		for(int j = i+1; j < cache.size(); j++){
    			Bigram temp = new Bigram();
    			temp.set(cache.get(i), cache.get(j));
    			context.write(temp, one);
    		}
    	}
        
    }
    
 }
 
 public static class IdMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 IntWritable res = new IntWritable();
		 //res.set(Integer.parseInt(value.toString()));
		 
		 
		 String temp = value.toString();
		 int x = Character.getNumericValue(temp.charAt(temp.length()-1));
		 res.set(x);
		 context.write(new Text("Totaal"), res);
	 }
 }
 
 public static class AggregateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	 public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		 int total = 0;
		 for(IntWritable value : values) {
			 total += value.get();
			 //context.write(new Text("Total"), new IntWritable(total));
		 }
		 
		 context.write(new Text("Total"), new IntWritable(total/6));
	 }
 }
        
 public static void main(String[] args) throws Exception {
	  
    Configuration conf = new Configuration();
     
    FileSystem fs = FileSystem.get(conf);
	fs.delete(new Path("temp1"), true);
    
    Job job = Job.getInstance(conf);
    job.setJobName("Counting");

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
        
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    //job.setJarByClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("temp1"));
    
    
    Job agg = Job.getInstance(conf);
    agg.setJobName("Aggregate");
    
    agg.setMapOutputKeyClass(Text.class);
    agg.setMapOutputValueClass(IntWritable.class);
    
    agg.setOutputKeyClass(Text.class);
    agg.setOutputValueClass(IntWritable.class);
    
    agg.setMapperClass(IdMapper.class);
    agg.setReducerClass(AggregateReducer.class);
    
    agg.setInputFormatClass(TextInputFormat.class);
    agg.setOutputFormatClass(TextOutputFormat.class);
    
    
    FileInputFormat.setInputPaths(agg, new Path("temp1"));
    FileOutputFormat.setOutputPath(agg, new Path(args[1]));
        
    job.waitForCompletion(true);
    agg.waitForCompletion(true);
 }
        
}
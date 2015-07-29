
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Actors {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
				
		public ActorList actors = new ActorList();
		public int actorSize = 0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String url = "";
			
			
			System.out.println();
			System.out.println("ACTOR SIZE IS ->" + actors.actors.size());
			System.out.println();

			if(line.toLowerCase().startsWith("warc-target-uri:")) {
				url = line.split(" ")[1];
			}

			ArrayList<Actor> curactors = actors.getCurrent();
			for(int i = 0; i < curactors.size(); i++){
				Actor actor = curactors.get(i);
				if(line.contains(actor.getName())){
					actors.found(i);
					context.write(new Text(url), new IntWritable(actor.getID()));
				}
			}
			
			if(line.toLowerCase().equals("warc/1.0")){
				actors.reset();
			}       
		}
	} 

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public IntWritable one = new IntWritable(1);

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			ArrayList<Integer> cache = new ArrayList<Integer>();

			for(IntWritable val:values) {
				cache.add(val.get());
			}

			for(int i = 0; i < cache.size(); i++){
				for(int j = i+1; j < cache.size(); j++){
					Bigram temp = new Bigram(cache.get(i), cache.get(j));
					context.write(new Text(temp.toString()), one);
				}
			}

		}

	}

	public static class IdMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			context.write(new Text(line.split("\\s+")[0] + " " + line.split("\\s+")[1]),new IntWritable(1));
		}
	}

	public static class AggregateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			for(IntWritable value : values) {
				total += value.get();
			}
			context.write(key, new IntWritable(total));
		}
	}

	public static void main(String[] args) throws Exception {
		
		
		Configuration conf = new Configuration();
		
		System.out.println("printing!!!!");
		ActorList lst = new ActorList();
		
		System.out.println("ACTORLIST -> " + lst.actors.size());
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("temp1"), true);

		Job job = Job.getInstance(conf);
		job.setJobName("Counting");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setJarByClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileStatus[] status = fs.listStatus(new Path("hdfs://hathi-surfsara/user/TUD-DS03/" + args[0]));
		for(int i=0; i<status.length; i++){
			FileStatus temp = status[i];
			System.out.println("1 van de path is: " +temp.getPath());
			FileInputFormat.addInputPath(job, temp.getPath());
		}
		FileOutputFormat.setOutputPath(job, new Path("temp1"));
		

		Configuration conf2 = new Configuration();

		Job agg = Job.getInstance(conf2);
		agg.setJobName("Aggregate");

		agg.setMapOutputKeyClass(Text.class);
		agg.setMapOutputValueClass(IntWritable.class);

		agg.setOutputKeyClass(Text.class);
		agg.setOutputValueClass(IntWritable.class);

		agg.setMapperClass(IdMapper.class);
		agg.setReducerClass(AggregateReducer.class);
		
		agg.setJarByClass(AggregateReducer.class);

		agg.setInputFormatClass(TextInputFormat.class);
		agg.setOutputFormatClass(TextOutputFormat.class);


		FileInputFormat.setInputPaths(agg, new Path("temp1"));
		FileOutputFormat.setOutputPath(agg, new Path(args[1]));

		job.waitForCompletion(true);
		agg.waitForCompletion(true);
	}

}

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

public class Actors2 {
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

		Job agg = Job.getInstance(conf);
		agg.setJobName("Aggregate");
		agg.setJarByClass(AggregateReducer.class);

		agg.setMapOutputKeyClass(Text.class);
		agg.setMapOutputValueClass(IntWritable.class);

		agg.setOutputKeyClass(Text.class);
		agg.setOutputValueClass(IntWritable.class);

		agg.setMapperClass(IdMapper.class);
		agg.setReducerClass(AggregateReducer.class);
		
		agg.setJarByClass(AggregateReducer.class);

		agg.setInputFormatClass(TextInputFormat.class);
		agg.setOutputFormatClass(TextOutputFormat.class);


		FileInputFormat.setInputPaths(agg, new Path(args[0]));
		FileOutputFormat.setOutputPath(agg, new Path(args[1]));

		agg.waitForCompletion(true);
	}

}
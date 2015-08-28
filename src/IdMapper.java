import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//this mapper basically maps the input one by one to the reducer class
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		context.write(new Text(line.split("\\s+")[0] + " " + line.split("\\s+")[1]), new IntWritable(1));
	}
}
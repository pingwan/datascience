import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.compress.GzipCodec;

import nl.surfsara.warcutils.WarcSequenceFileInputFormat;

public class Runner {
	static String datapath = "/data/public/common-crawl/crawl-data/CC-MAIN-2014-10/segments/*/seq/*";
//	static String datapath = "/data/public/common-crawl/crawl-data/CC-MAIN-2014-10/segments/1393999670363/seq/CC-MAIN-20140305060750-00052-ip-10-183-142-35.ec2.internal.warc.seq";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//JOB ONE
		//init job
		Job jobone = Job.getInstance(conf);
		jobone.setJobName("Linked actors - Job one");
		jobone.setJarByClass(Runner.class);
		
		//set input and output path
		FileInputFormat.addInputPath(jobone, new Path("hdfs://hathi-surfsara/" + datapath));
		FileOutputFormat.setOutputPath(jobone, new Path(args[0]));

		//set mappers + reducers
		jobone.setMapperClass(ExtractActors.class);
		jobone.setReducerClass(AggregateActors.class);
		
		//set input class
		jobone.setInputFormatClass(WarcSequenceFileInputFormat.class);
		
		//set output classes
		jobone.setOutputKeyClass(Text.class);
		jobone.setOutputValueClass(IntWritable.class);
		
		jobone.waitForCompletion(true);
		
		//JOB TWO
		//init
		Job jobtwo = Job.getInstance(conf);
		jobtwo.setJobName("Linked actors - Job two");
		jobtwo.setJarByClass(Runner.class);
		
		//set input and output path
		FileInputFormat.addInputPath(jobtwo, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobtwo, new Path(args[1]));
		
		//set map/reduce classes
		jobtwo.setMapperClass(IdMapper.class);
		jobtwo.setReducerClass(AggregateBigrams.class);
		
		//set input/output formats
		jobtwo.setInputFormatClass(TextInputFormat.class);
		jobtwo.setOutputKeyClass(Text.class);
		jobtwo.setOutputValueClass(IntWritable.class);

		//compress output
		FileOutputFormat.setCompressOutput(jobtwo, true);
		FileOutputFormat.setOutputCompressorClass(jobtwo, GzipCodec.class);
		
		jobtwo.waitForCompletion(true);
	}
}

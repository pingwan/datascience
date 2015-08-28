import java.io.IOException;
import java.util.ArrayList;

import objects.Bigram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateActors extends Reducer<Text, IntWritable, Text, IntWritable> {
	public IntWritable one = new IntWritable(1);

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//needed to iterate
		ArrayList<Integer> cache = new ArrayList<Integer>();

		//loop through all the found actors and add them to the arraylist
		for(IntWritable val:values) {
			if(!cache.contains(val.get())) {
				cache.add(val.get());
			}
		}

		//generate all possible bigrams (containing the ids of the actors) from the found actors 
		for(int i = 0; i < cache.size(); i++){
			for(int j = i+1; j < cache.size(); j++){
				Bigram temp = new Bigram(cache.get(i), cache.get(j));
				context.write(new Text(temp.toString()), one);
			}
		}

	}
}
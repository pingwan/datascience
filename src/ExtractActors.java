import java.io.IOException;
import java.util.ArrayList;

import objects.Actor;
import objects.ActorList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.jwat.warc.WarcRecord;
import org.jwat.common.Payload;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class ExtractActors extends Mapper<LongWritable, WarcRecord, Text, IntWritable>{
	public ActorList actors = new ActorList();
	public int actorSize = 0;

	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		// Only process http responses
		if ("application/http; msgtype=response".equals(value.header.contentTypeStr)) {
			Payload p = value.getPayload();
			
			if(p != null) {
				String content = IOUtils.toString(p.getInputStreamComplete());
				
				if (content != null && content != "") {
					Document doc = Jsoup.parse(content);
					//we only look for actor occurences in the title
					String title = doc.title();
					
					if(title != "") {
						String url = value.header.warcTargetUriStr;
						
						ArrayList<Actor> actorlist = actors.getActors();
						for(int i = 0; i < actorlist.size(); i++){
							Actor actor = actorlist.get(i);
							if(title.toLowerCase().contains(actor.getName().toLowerCase())){
								context.write(new Text(url), new IntWritable(actor.getID()));
							}
						}
					}
				}
			}    
		}
	}
}
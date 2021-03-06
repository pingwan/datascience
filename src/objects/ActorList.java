package objects;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ActorList {
	public ArrayList<Actor> actors;
	
	public ActorList(){
		actors = new ArrayList<Actor>();

		try {
			Path pt = new Path("hdfs://hathi-surfsara/user/TUD-DS03/actors.txt");
			FileSystem fs = FileSystem.get(new Configuration());
		
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			while((line = br.readLine())!=null){
				String[] parts = line.split(" ");
				actors.add(new Actor(Integer.parseInt(parts[0]), parts[1]));
			}
			
			br.close();
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Actor get(int index) {
		return actors.get(index);
	}
	
	public ArrayList<Actor> getActors(){
		return actors;
	}
}

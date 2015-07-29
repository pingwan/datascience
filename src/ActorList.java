import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ActorList {
	public ArrayList<Actor> actors;
	public ArrayList<Actor> current;
	
	public ActorList(){
		actors = new ArrayList<Actor>();
		current = new ArrayList<Actor>();

		try {
			Path pt = new Path("hdfs://hathi-surfsara/user/TUD-DS03/actors.txt");
			FileSystem fs = FileSystem.get(new Configuration());
		
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			while((line = br.readLine())!=null){
				String[] parts = line.split(" ");
				actors.add(new Actor(Integer.parseInt(parts[0]), parts[1]));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void found(int index) {
		current.remove(index);
	}
	
	public void reset(){
		current = actors;
	}
	
	public ArrayList<Actor> getCurrent(){
		return current;
	}
}

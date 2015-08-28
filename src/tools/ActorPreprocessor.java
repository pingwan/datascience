package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class ActorPreprocessor {
	
	public static void main(String[] args) {
		File actors = new File("actors_stripped.list");
			
		try {
			PrintWriter writer = new PrintWriter("actors_nw.list");
			List<String> actorlst = getActors(actors);
			int i = 1191592;
			System.out.println(actorlst.size());
			for(String actor : actorlst) {
				writer.println(i + " " + actor);	
				i++;
			}
			writer.flush();
			writer.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static List<String> getActors(File f) throws FileNotFoundException {
		List<String> result = new ArrayList<String>();
		
		try{
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
			while((line = br.readLine())!= null) {
				if(!line.startsWith("\t") && line.length() > 0){
					String act = line.split("\\t")[0];
					String[] parts = act.split(",");
					if(parts.length>1) {
						result.add(parts[1].trim()+" "+parts[0].trim());
					} else{
						result.add(parts[0].trim());
					}
						
				}
			}
			
			br.close();
		} 
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
		return result;
	}

}

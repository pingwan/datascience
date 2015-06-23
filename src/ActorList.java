import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class ActorList {

	public ArrayList<String> actors;
	public ArrayList<String> current;
	
	public ActorList(){
		actors = new ArrayList<String>();
		current = new ArrayList<String>();
		File f = new File("actors.txt");
		
		try {
			Scanner sc = new Scanner(f);
			while(sc.hasNext()) {
				actors.add(sc.next());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//current = actors;
	}
	
	public void found(String actor) {
		/*int idx = current.indexOf(actor);
		if(idx > -1) {
			actors.remove(idx);
		}*/
		current.add(actor);
	}
	
	public void reset(){
		current.clear();
	}
	
	public ArrayList<String> getActors(){
		return actors;
	}
		
	
}

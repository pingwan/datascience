import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class ActorList {
	public ArrayList<Actor> actors;
	public ArrayList<Actor> current;
	
	public ActorList(){
		actors = new ArrayList<Actor>();
		current = new ArrayList<Actor>();
		File f = new File("actors.txt");
		
		try {
			Scanner sc = new Scanner(f);
			while(sc.hasNext()) {
				actors.add(new Actor(sc.nextInt(), sc.next()));
			}
		} catch (FileNotFoundException e) {
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

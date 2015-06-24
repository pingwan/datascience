
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Bigram implements WritableComparable {    
	private IntWritable actorA;
	private IntWritable actorB;

	public Bigram(IntWritable a, IntWritable b) {
		this.actorA = a;
		this.actorB = b;
	}
	
	public Bigram() {
		this.actorA = new IntWritable();
		this.actorB = new IntWritable();
	}
	
	public void write(DataOutput out) throws IOException {
		actorA.write(out);
		actorB.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		actorA.readFields(in);
		actorB.readFields(in);
	}

	public String getA() {
		return this.actorA.toString();
	}

	public String getB() {
		return this.actorB.toString();
	}

	public static Bigram read(DataInput in) throws IOException {
		Bigram w = new Bigram();
		w.readFields(in);
		return w;
	}
	
	public int compareTo(Object o) {
		if(o instanceof Bigram){
			Bigram temp = (Bigram) o;
			
			if(this.actorA.equals(temp.actorB) && this.actorB.equals(temp.actorB)){
				return 1;
			} 
			else{
				return 0;
			}
		}
		else{
			return 0;
		}
	}
	
	public int hashCode() {
		return (this.actorA.hashCode() / this.actorA.hashCode());
	}

	@Override
	public String toString(){
		return this.actorA.toString() + " " + this.actorB.toString();
	}
}
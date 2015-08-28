package objects;

public class Bigram {    
	private int actorA;
	private int actorB;

	public Bigram(int a, int b) {
		if(a<=b) {
			this.actorA = a;
			this.actorB = b;
		} else {
			this.actorA = b;
			this.actorB = a;
		}
	}
	
	public Bigram() {
		super();
	}
	
	public int getA() {
		return this.actorA;
	}

	public int getB() {
		return this.actorB;
	}
	@Override
	public String toString(){
		return this.actorA + " " + this.actorB;
	}
}
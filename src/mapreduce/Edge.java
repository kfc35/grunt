package mapreduce;

public class Edge {

	public long from;
	public long to;
	public long prob;
	
	public Edge(long f, long t, long p) {
		from = f;
		to = t;
		prob = p;
	}

	public long getProb() {
		return prob;
	}

	public void setProb(long prob) {
		this.prob = prob;
	}
}

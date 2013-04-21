package mapreduce;

public class Edge {

	private long from;
	private long to;
	private long prob;
	
	public Edge(long f, long t, long p) {
		from = f;
		to = t;
		prob = p;
	}

	public long getFrom() {
		return from;
	}
	
	public void setFrom(long from) {
		this.from = from;
	}

	public long getTo() {
		return to;
	}

	public void setTo(long to) {
		this.to = to;
	}



	public long getProb() {
		return prob;
	}

	public void setProb(long prob) {
		this.prob = prob;
	}
}

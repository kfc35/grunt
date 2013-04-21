package mapreduce;

import java.util.ArrayList;

public class Node {

	private long id;
	private long pageRank = ((long) 1) / ((long) 685230);
	private ArrayList<Edge> inEdges;
	private ArrayList<Edge> outEdges;
	
	public Node(long i) {
		id = i;
	}
	
	public long getPageRank() {
		return pageRank;
	}
	public void setPageRank(long pageRank) {
		this.pageRank = pageRank;
	}
	
	public void addInEdge(Edge n) {
		inEdges.add(n);
	}
	
	public void addOutEdge(Edge n) {
		outEdges.add(n);
	}
}

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
	
	public long getId() {
		return id;
	}

	/*public void setId(long id) {
		this.id = id;
	}*/

	public long getPageRank() {
		return pageRank;
	}
	public void setPageRank(long pageRank) {
		this.pageRank = pageRank;
	}
	
	public ArrayList<Edge> getOutEdges() {
		return outEdges;
	}
	
	public void addInEdge(Edge n) {
		inEdges.add(n);
	}
	
	public void addOutEdge(Edge n) {
		outEdges.add(n);
	}
	
	public boolean equals(Object o) {
		if (o instanceof Node) {
			return ((Node) o).getId() == this.id;
		}
		else return false;
	}
}

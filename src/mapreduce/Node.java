package mapreduce;

import java.util.ArrayList;

public class Node {

	private long id;
	private long pageRank = ((long) 1) / ((long) 685230);
	private ArrayList<Long> inNodes;
	private ArrayList<Long> outNodes;
	
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
	
	public ArrayList<Long> getOutNodes() {
		return outNodes;
	}
	
	public long getNumOuts() {
		return (long) outNodes.size();
	}
	
	public void addInNode(Long n) {
		inNodes.add(n);
	}
	
	public void addOutNode(Long n) {
		outNodes.add(n);
	}
	
	public boolean equals(Object o) {
		if (o instanceof Node) {
			return ((Node) o).getId() == this.id;
		}
		else return false;
	}
}

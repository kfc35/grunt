package mapreduce;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;


public class SimpleMapReduce {

	final static String FILE_NAME = "output.txt";
	final static Charset ENCODING = StandardCharsets.UTF_8;
	public static Node[] nodes;
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws MessagingException 
	 * @throws AddressException 
	 */
	public static void main(String[] args) throws IOException, AddressException, MessagingException {
		try {
		nodes = new Node[685230];
		
		Util.Init();
		
		} catch (Exception e) {}
		Util.email();
	}
	
	/*The return value is a list of nodes, where only the ID and the page rank value is specified
	 *The mapper receives the nodeId as the key, and the node object as the value.
	 *The node object contains the edges and the probability distribution*/
	public static ArrayList<Node> mapper(int nodeId, Node node) {
		ArrayList<Node> flowingPagerank = new ArrayList<Node>();
		for (Edge edge : node.getOutEdges()) {
			//Assume only one edge exists to a connected node.
			Node destination = new Node(edge.getTo());
			destination.setPageRank(edge.getProb() * node.getPageRank());
			flowingPagerank.add(destination);
		}
		return flowingPagerank;
	}
	
	/*The return value is the pageRank for the given nodeId.
	 *The reducer receives the nodeId as the key, and a intermediary node object as the value.
	 *The node object contains only the page rank value flowing into it from some other node.
	 */
	public static long reducer(int nodeId, ArrayList<Node> pageRankValues) {
		long pageRankValue = 0;
		for (Node pageRankValueNode : pageRankValues) {
			pageRankValue += pageRankValueNode.getPageRank();
		}
		return pageRankValue;
	}
}

package fileGen;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class kfcFileGen {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		//hhc39
//		double rejectMin = 0.99 * 0.93;
//		double rejectLimit = rejectMin + 0.01;
//		
//		BufferedReader reader = new BufferedReader(new FileReader("edges.txt"));
//		BufferedReader sweet = new BufferedReader(new FileReader("hadoopInput.txt"));
//		
//		String line = reader.readLine();
//		String sweetsLine = sweet.readLine();
//		int currentNode = 0;
//		HashSet<Integer> outs = new HashSet<Integer>();
//		
//		while (line != null) {
//			int fromNode = Integer.parseInt(line.substring(0, 6).trim());
//			int toNode = Integer.parseInt(line.substring(7, 7+6).trim());
//			double x = Double.parseDouble(line.substring(15).trim());
//			
//			if (x >= rejectMin && x < rejectLimit) {
//				line = reader.readLine();
//				continue;
//			}
//			
//			if (fromNode != currentNode) {
//				//do Comparison with sweet's line.
//				String[] keyVal = sweetsLine.split("\t");
//				if (Integer.parseInt(keyVal[0]) % 100000 == 0) {
//					System.out.println("Current line in sweet's file: " + keyVal[0] + "_" + keyVal[1]);
//				}
//				if (Integer.parseInt(keyVal[0]) != currentNode) {
//					System.out.println("You wrote the program wrong, Kevin. keyVal[0] = " 
//				+ keyVal[0] + ", currentNode = " + currentNode);
//				}
//				
//				String[] PRNumOutsAndOuts = keyVal[1].split(" ");
//				int numOuts = Integer.parseInt(PRNumOutsAndOuts[1]);
//				if (numOuts != PRNumOutsAndOuts.length - 2) {
//					System.out.println("Internal Discrepancy at currentNode = " + currentNode + 
//							": numOuts != the number of outgoing edges");
//				}
//				for (int i = 2; i < PRNumOutsAndOuts.length; i++) {
//					if (!outs.contains(Integer.parseInt(PRNumOutsAndOuts[i]))) {
//						System.out.println("currentNode = " + currentNode + " has an edge in HadoopInput " +
//								"that does not exist in edges.txt: " + Integer.parseInt(PRNumOutsAndOuts[i]));
//					}
//				}
//				if (PRNumOutsAndOuts.length - 2 != outs.size()) {
//					System.out.println("currentNode = " + currentNode + " does not have all the edges " +
//							"that exist in edges.txt possibly (size in HadoopInput is less)");
//				}
//				
//				
//				/*Nodes that don't have out edges are omitted from the file*/
//				currentNode++;
//				sweetsLine = sweet.readLine();
//				while (sweetsLine != null && currentNode != fromNode) {
//					
//					keyVal = sweetsLine.split("\t");
//					PRNumOutsAndOuts = keyVal[1].split(" ");
//					if (Integer.parseInt(PRNumOutsAndOuts[1]) != 0) {
//						System.out.println("currentNode = " + currentNode + " has no outgoing " +
//								"edges in edges.txt, but has edges in hadoopInput.txt");
//					}
//					if (Integer.parseInt(keyVal[0]) != currentNode) {
//						System.out.println("You wrote the program wrong, Kevin. keyVal[0] = " 
//								+ keyVal[0] + ", currentNode = " + currentNode);
//					}
//					currentNode++;
//					sweetsLine = sweet.readLine();
//				}
//				outs = new HashSet<Integer>();
//			}
//			
//			outs.add(new Integer(toNode));
//			
//			
//			line = reader.readLine();
//		}
		//Do last comparison
		
		String a = "1 2 3 ";
		StringTokenizer itr = new StringTokenizer(a);
		while (itr.hasMoreElements()) {
			System.out.println("itr has: " + itr.nextToken());
		}
	}

}

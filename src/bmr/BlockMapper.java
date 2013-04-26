package bmr;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.Util;

public class BlockMapper extends Mapper<Text, Text, Text, Text> {

	public BlockMapper() {}

	/**
	 * The return value is a list of nodes, where only the ID and the page rank value is specified
	 * The mapper receives the nodeId as the key, and the node object as the value.
	 * The node object contains the edges and the probability distribution
	 */
	protected void map(Text key, Text value, 
			Context context) throws IOException, InterruptedException {
		
		// You to yourself for residual comparison
		/*
		 * Value would be
		 * origin nodeID
		 * pagerank
		 * numOuts
		 * list of outs (if exists)
		 */
		context.write(Util.blockIDofNode(Long.valueOf(key.toString())), 
				new Text(key.toString() + " " + value.toString()));

		/*
		 * Value should be in the form:
		 * pagerank
		 * numOuts
		 * list of outs
		 */
		StringTokenizer itr = new StringTokenizer(value.toString());

		// Get the long value of the pagerank	
		Float pageRank = Float.valueOf(itr.nextToken());

		// Get the number of outgoing edges
		Float numOuts = Float.valueOf(itr.nextToken());

		
//		 /* There's only you... so sad, so sad
//		 * pagerank
//		 * 0
//		 */
//		if (numOuts == (float) 0) {
//			
//			 /* Value in the form of 
//			 * origin = destination nodeID
//			 * pageRank left for oneself
//			 */
//			context.write(Util.blockIDofNode(Long.valueOf(key.toString())), 
//					new Text(key.toString() + " " + pageRank.toString()));
//		} else {	
			// Compute the pagerank to all output edges
			Text outRankText = new Text(Float.valueOf(pageRank / numOuts).toString());

			while (itr.hasMoreTokens()) {
				String nextKey = itr.nextToken().toString();
				Text toBlockID = Util.blockIDofNode(Long.valueOf(nextKey));
				
				if (!toBlockID.toString().equals(Util.blockIDString(Long.valueOf(key.toString())))) {
				
				 /* Value in the form of 
				 * destination nodeID
				 * pageRank for that node
				 */
				context.write(toBlockID, 
						new Text(nextKey + " " + outRankText.toString()));
				}
			}
//		}
	}
}

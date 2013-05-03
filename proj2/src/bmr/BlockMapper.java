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
		Text selfBlockID = Util.blockIDofNode(Long.valueOf(key.toString()));
		Integer selfBlockInt = Integer.valueOf(selfBlockID.toString());

		/*
		 * The first value of the value is the identification
		 * -1 means writing input information to itself
		 * -1 nodeID (input value = pagerank numOuts list)
		 * 0 means an edge from within the block
		 * 0 toNodeID fromNodeID
		 * 1 means pagerank from outside the block
		 * 1 toNodeID fromPageRankFlow
		 */

		// You to yourself for residual comparison
		context.write(selfBlockID, new Text("-1 " + key.toString() + " " + value.toString()));

		/*
		 * Value should be in the form:
		 * pagerank
		 * numOuts
		 * list of outs
		 */
		StringTokenizer itr = new StringTokenizer(value.toString());

		// Get the long value of the pagerank	
		Double pageRank = Double.valueOf(itr.nextToken());

		// Get the number of outgoing edges
		Double numOuts = Double.valueOf(itr.nextToken());


		/* There's only you... so sad, so sad
		 * Add your pagerank to the pagerankvalues enum
		 */
		if (numOuts == 0.0) {
			context.getCounter(BlockMapReduce.GraphCounters.MASTER_NO_OUTS_PR).increment((long) (pageRank * 10E12));
		} else {	
			// Compute the pagerank to all output edges
			Text outRankText = new Text(Double.valueOf(pageRank / numOuts).toString());

			while (itr.hasMoreTokens()) {
				String nextKey = itr.nextToken();
				Text toBlockID = Util.blockIDofNode(Long.valueOf(nextKey));

				// Write the pagerank to the other block node
				if (Integer.parseInt(toBlockID.toString()) != selfBlockInt) {

					context.write(toBlockID, 
							new Text("1 " + nextKey + " " + outRankText.toString()));
				} else {
					context.write(toBlockID, 
							new Text("0 " + nextKey + " " + key.toString()));
				}
			}
		}
	}
}

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
		context.write(Util.blockIDofNode(Long.valueOf(key.toString())), new Text(key.toString() + " " + value.toString()));

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

		/* There's only you... so sad, so sad
		 * pagerank
		 * 0
		 */
		if (numOuts == (float) 0) {
			context.write(Util.blockIDofNode(Long.valueOf(key.toString())), new Text(key.toString() + " " + pageRank.toString()));
		} else {	
			// Compute the pagerank to all output edges
			Text outRankText = new Text(Float.valueOf(pageRank / numOuts).toString());

			while (itr.hasMoreTokens()) {
				String nextKey = itr.nextToken().toString();
				context.write(Util.blockIDofNode(Long.valueOf(nextKey)), new Text(nextKey + " " + outRankText.toString()));
			}
		}
	}
}

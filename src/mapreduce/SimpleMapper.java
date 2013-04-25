package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMapper extends Mapper<Text, Text, Text, Text> {

	public SimpleMapper() {}

	/**
	 * The return value is a list of nodes, where only the ID and the page rank value is specified
	 * The mapper receives the nodeId as the key, and the node object as the value.
	 * The node object contains the edges and the probability distribution
	 */
	protected void map(Text key, Text value, 
			Context context) throws IOException, InterruptedException {
		// You to yourself for residual comparison
		context.write(key, value);

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
			context.write(key, new Text(pageRank.toString()));
		} else {
			// If your pagerank is 0, then you're useless
			if (pageRank != 0) {
				// Compute the pagerank to all output edges
				Text outRankText = new Text(Float.valueOf(pageRank / numOuts).toString());

				while (itr.hasMoreTokens()) {
					context.write(new Text(itr.nextToken()), outRankText);
				}
			}
		}
	}
}

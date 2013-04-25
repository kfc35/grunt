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

		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		
		Text k = new Text(Long.valueOf(key.toString()).toString());

		// Get the long value of the pagerank	
		Float v = Float.valueOf(itr.nextToken().toString());
		float prev = 1 + v;

		// You to yourself for residual comparison
		context.write(k, 
				new Text("" + prev + " " + line.split(" ", 2)[1]));		

		// Get the number of outgoing edges
		Float numOuts = Float.valueOf(itr.nextToken().toString());

		// There's only you... so sad, so sad
		if (numOuts == 0) {
			context.write(k, new Text(v.toString()));
		} else {
			// Compute the pagerank to all output edges
			Text flowText = new Text(Float.valueOf(v / numOuts).toString());

			while (itr.hasMoreTokens()) {
				context.write(new Text(Long.valueOf(itr.nextToken().toString()).toString()), flowText);
			}
		}
	}
}

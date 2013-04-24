package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMapper extends Mapper<Text, Text, LongWritable, Text> {

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

		// Get the long value of the pagerank	
		Float v = Float.valueOf(itr.nextToken().toString());
		float prev = 1 + v;
		
		// Compute the pagerank to all output edges
		Float flow = new Float(v / Float.valueOf(itr.nextToken().toString()));
		Text flowText = new Text(flow.toString());
		// You to yourself for residual comparison
		context.write(new LongWritable(Long.valueOf(key.toString())), 
				new Text("" + prev + " " + line.split(" ", 2)[1]));
		
		while (itr.hasMoreTokens()) {
			context.write(new LongWritable(Long.valueOf(itr.nextToken().toString())), flowText);
		}
	}
}

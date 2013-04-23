package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMapper extends Mapper<Text, Text, LongWritable, LongWritable> {

	public SimpleMapper() {}

	/**
	 * The return value is a list of nodes, where only the ID and the page rank value is specified
	 * The mapper receives the nodeId as the key, and the node object as the value.
	 * The node object contains the edges and the probability distribution
	 */
	protected void map(Text key, Text value, 
			OutputCollector<LongWritable, LongWritable> output, 
			Reporter reporter) throws IOException, InterruptedException {
		
		LongWritable origin = new LongWritable(Double.valueOf(key.toString()).longValue());
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);

		// Get the long value of the pagerank	
		long v = Double.valueOf(itr.nextToken().toString()).longValue();
		// COmpute the pagerank to all output edges
		LongWritable flow = new LongWritable(v / Double.valueOf(itr.nextToken().toString()).longValue());
		// You to yourself for residual comparison
		output.collect(origin, new LongWritable(1 + v));
		
		while (itr.hasMoreTokens()) {
			LongWritable link = new LongWritable(Double.valueOf(itr.nextToken().toString()).longValue());
			output.collect(link, flow);
		}
	}
}

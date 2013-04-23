package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	public SimpleMapper() {}

	/**
	 * The return value is a list of nodes, where only the ID and the page rank value is specified
	 * The mapper receives the nodeId as the key, and the node object as the value.
	 * The node object contains the edges and the probability distribution
	 */
	protected void map(LongWritable key, Text value, 
			OutputCollector<LongWritable, LongWritable> output, 
			Reporter reporter) throws IOException, InterruptedException {
		// Get the long value of the pagerank	
		long v = Double.valueOf(value.toString()).longValue();

		Node node = SimpleMapReduce.nodes[(int) key.get()];
		// COmpute the pagerank to all output edges
		LongWritable flow = new LongWritable(v / node.getNumOuts());

		for (Long to : node.getOutNodes()) {	
			LongWritable link = new LongWritable(to);
			output.collect(link, flow);
		}
	}
}

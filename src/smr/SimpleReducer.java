package smr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.Util;

public class SimpleReducer extends Reducer<Text, Text, Text, Text> {
	
	public SimpleReducer() {}
	/**
	 * The return value is the pageRank for the given nodeId.
	 * The reducer receives the nodeId as the key, and a intermediary node object as the value.
	 * The node object contains only the page rank value flowing into it from some other node.
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	protected void reduce(Text key, java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		float pageRankValue = 0;
		float previous = 0;
		String rest = "";
		
		// Iterate through all the values
		for (Text t : values) {
			String v = t.toString();
			/*
			 * pagerank
			 * num [list of outgoing nodes] (if exists)
			 */
			String[] args = v.split(" ", 2);
			
			Float rank = Float.valueOf(args[0]);

			// If it was to itself for residual computations
			if (args.length > 1) {
				previous = rank;
				rest = args[1];
			} else {
				pageRankValue += rank;
			}
		}
		
		pageRankValue = Util.dis + Util.damping * pageRankValue;
		
		// Calculate the residual, if zero new residual, then change is 100%
		//float thisResidual = 100;
		//if (pageRankValue != 0) {
		float thisResidual = Math.abs((previous - pageRankValue))/pageRankValue;
		//}
		
		// Increment by this long residual
		context.getCounter(SimpleMapReduce.GraphCounters.RESIDUAL).increment((long) thisResidual);
		context.getCounter(SimpleMapReduce.GraphCounters.NODES).increment(1);
		
		// Write out for next iteration
		Text out = new Text("" + pageRankValue + " " + rest);
		context.write(key, out);
	}
}

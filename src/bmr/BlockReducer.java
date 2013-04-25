package bmr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.Util;

public class BlockReducer extends Reducer<Text, Text, Text, Text> {
	
	public BlockReducer() {}
	
	/**The value in the BlockReducer has the nodeID appended to the beginning
	 * This is because, now that the mapper maps to blocks, you need the nodeID information
	 * somewhere else (not in the key, but the value)
	 */
	
	protected void reduce(Text key, java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		//Array of max block size to map nodes to
		
		float pageRankValue = 0;
		float previous = 0;
		String rest = "";
		//long nodeID;
		
		// Iterate through all the values
		for (Text t : values) {
			String v = t.toString();
			
			/*
			 * nodeID
			 * pagerank
			 * num [list of outgoing nodes] (if exists)
			 */
			
			String[] args = v.split(" ", 3);
			
			Float rank = Float.valueOf(args[1]);

			// If it was to itself for residual computations
			if (args.length > 2) {
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
		context.getCounter(BlockMapReduce.GraphCounters.RESIDUAL).increment((long) thisResidual);
		context.getCounter(BlockMapReduce.GraphCounters.NODES).increment(1);
		
		// Write out for next iteration
		Text out = new Text("" + pageRankValue + " " + rest);
		context.write(key, out);
	}

}

package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
		
		float pageRankValue = (float)0;
		float previous = (float) 0;
		String rest = "";
		
		// Iterate through all the values
		for (Text t : values) {
			String v = t.toString();
			String[] args = v.split(" ", 2);
			Float rank = Float.valueOf(args[0]);;

			// If it was to itself for residual computations
			if (rank > 1) {
				previous = rank - 1;
				rest = args[1];
			} else {
				pageRankValue += rank;
			}
		}
		
		// Calculate the residual
		float thisResidual = Math.abs((pageRankValue - previous))/pageRankValue;
		
		// Increment by this long residual
		context.getCounter(SimpleMapReduce.GraphCounters.RESIDUAL).increment((long) thisResidual);
		context.getCounter(SimpleMapReduce.GraphCounters.NODES).increment(1);
		
		// Write out for next iteration
		Text out = new Text("" + pageRankValue + " " + rest);
		context.write(new Text(key.toString()), out);
	}
}

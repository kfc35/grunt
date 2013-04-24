package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleReducer extends Reducer<LongWritable, Text, Text, Text> {

	public SimpleReducer() {}
	/**
	 * The return value is the pageRank for the given nodeId.
	 * The reducer receives the nodeId as the key, and a intermediary node object as the value.
	 * The node object contains only the page rank value flowing into it from some other node.
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	protected void reduce(LongWritable key, Iterator<Text> values, 
			Context context) throws IOException, InterruptedException {
		
		float pageRankValue = (float)0;
		float previous = (float) 0;
		String rest = "";
		while (values.hasNext()) {
			String v = values.next().toString();
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
		//float thisResidual = (pageRankValue - previous)/pageRankValue;
		//long longResidual = (long) (thisResidual * 10E12);
		context.getCounter(SimpleMapReduce.GraphCounters.RESIDUAL).increment(1);
		/*
		context.getCounter(SimpleMapReduce.GraphCounters.MIN_RESIDUAL).setValue(
				Math.min(context.getCounter(SimpleMapReduce.GraphCounters.MIN_RESIDUAL).getValue(), longResidual));
				*/
		Text out = new Text("" + pageRankValue + " " + rest);
		
		context.write(new Text(key.toString()), out);
	}
}

package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	public SimpleReducer() {}
	/**
	 * The return value is the pageRank for the given nodeId.
	 * The reducer receives the nodeId as the key, and a intermediary node object as the value.
	 * The node object contains only the page rank value flowing into it from some other node.
	 * @throws IOException 
	 */
	protected void reduce(LongWritable key, Iterator<LongWritable> values, 
			OutputCollector<LongWritable, LongWritable> output, 
			Reporter reporter) throws IOException {
		long pageRankValue = (long) 0.0;
		while (values.hasNext()) {
			pageRankValue += values.next().get();
		}
		output.collect(key, new LongWritable(pageRankValue));
	}
}

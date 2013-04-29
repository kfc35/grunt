package bmr;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

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
		
		Long blockID = Long.valueOf(key.toString());
		Long beginningNodeID = new Long(blockID == 0 ? 0 : Util.blocks[blockID.intValue() - 1] + 1);
		Long endingNodeID = new Long(Util.blocks[blockID.intValue()]);
		int size = endingNodeID.intValue() - beginningNodeID.intValue() + 1;

		//Array of max block size to map nodes to
		double[] beginningPR = new double[size];
		double[] PR = new double[size];
		double[] NPR = new double[size];

		// Array of the original mapper values for this node except the pagerank
		/*
		 * NumOuts
		 * List of Outs
		 */
		String[] originalValues = new String[size];
		Arrays.fill(originalValues, 0, size, "");

		// Array of the total incoming pagerank from outside node
		double[] boundaryPR = new double[size];

		// Array of incoming edges
		String[] originNodes = new String[size];
		Arrays.fill(originNodes, 0, size, "");

		/**Need first iteration to set everything up from the reducer**/
		SetUp(key, values, beginningPR, NPR, originalValues, boundaryPR, 
				originNodes, beginningNodeID, context);
		
		double residual = 0;

		int iteration = 1;
		/*
		 * Calculate the pageranks until convergence or until the residual is under a threshold
		 */
		do {
			iteration += 1;
			PR = NPR;
			NPR = new double[size];
			residual = IterateBlockOnce(beginningPR, PR, NPR, boundaryPR, 
					originalValues, originNodes, beginningNodeID, context);

			// TODO: Change when wanted more than 1 iteration
		} while (iteration <= 0 && (residual / (double) NPR.length) > 0.001);

		// Add the total block residual
		context.getCounter(BlockMapReduce.GraphCounters.RESIDUAL).increment((long) (residual * 10E7));
		context.getCounter(BlockMapReduce.GraphCounters.BLOCKS).increment(1);

		WriteKeyValue(context, beginningNodeID, NPR, originalValues);
	}
	
	/**
	 * 
	 * Iterates through the incoming values and parses them correctly
	 */
	private void SetUp(Text key, java.lang.Iterable<Text> values, double[] beginningPR, 
			double[] NPR, String[] originalValues, double[] boundaryPR, 
			String[] originNodes, Long beginningNodeID, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) {
		
		Integer block = Long.valueOf(key.toString()).intValue();
		
		for (Text t : values) {
			String v = t.toString();

			StringTokenizer st = new StringTokenizer(v);
			// 0 is the types
			int type = Integer.parseInt(st.nextToken());
			// 1 is the toNodeID
			Long nodeID = Long.valueOf(st.nextToken());
			
			// If it's the wrong block, add to the list and exit
			//TODO wrong if statement
			if ((block == 0 && nodeID > Util.blocks[0]) 
					|| (block != 0 && (nodeID > Util.blocks[block] || nodeID <= Util.blocks[block - 1]))) {
				context.getCounter(BlockMapReduce.GraphCounters.WRONG_BLOCK).increment(1);
				continue;
			}
			
			int offset = nodeID.intValue() - beginningNodeID.intValue();
			Double rank;

			// If this value is just the type setting value
			if (type == -1) {
				// 2 is the Pagerank
				rank = Double.valueOf(st.nextToken());
				beginningPR[offset] = rank;
				NPR[offset] = rank;

				// 3 is the original value to the mapper
				originalValues[offset] = v.split(" ", 4)[3];
			} else if (type == 0) {
				// If this value is from another node in the block
				originNodes[offset] += st.nextToken() + " ";
			} else {
				rank = Double.valueOf(st.nextToken());
				// Then it's a boundary PR
				boundaryPR[offset] += rank;
			}
		}
	}

	private double IterateBlockOnce(double[] beginningPR, double[] PR, double[] NPR, 
			double[] boundaryPR, String[] originalValues, String[] originNodes, 
			Long beginningNodeID, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) 
					throws IOException, InterruptedException {
		double residual = 0;

		// Iterate through all the nodes in the block
		//for (int i = 0 ; i < Util.size ; i++) {
		for (int i = 0 ; i < NPR.length ; i++) {

			// Always add the boundary flow into this block
			NPR[i] += boundaryPR[i];

			StringTokenizer itr = new StringTokenizer(originNodes[i]);
			while (itr.hasMoreTokens()) {

				// Get the source of the edge
				Long edgeFromID = Long.valueOf(itr.nextToken());
				int offset = edgeFromID.intValue() - beginningNodeID.intValue();

				// Get the previous pagerank of that source
				double edgeFromPageRank = PR[offset];

				// Get the numOuts of that source
				long numOuts = Long.valueOf(originalValues[offset].split(" ")[0]);

				// If this origin doesn't go out, then it all goes to me/itself
				if (numOuts == 0) {
					numOuts = 1;
				}
				// Add the flow to me
				NPR[i] += edgeFromPageRank / ((double) numOuts);
			}

			// Damping
			NPR[i] = Util.dis + Util.damping * NPR[i];
			residual += Math.abs(PR[i] - NPR[i]) / NPR[i];
		}

		return residual;
	}

	/**
	 * 
	 * @param context
	 * @param beginningNodeID
	 * @param NPR
	 * @param originalValues
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * Write out each key and value pair
	 */
	private void WriteKeyValue(org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context, 
			long beginningNodeID, double[] NPR, String[] originalValues) 
					throws IOException, InterruptedException {
		//for (int i = 0 ; i < Util.size ; i++) {
		for (int i = 0 ; i < NPR.length ; i++) {

			context.getCounter(BlockMapReduce.GraphCounters.TOTAL_PAGERANK).increment((long) (NPR[i] * 10E7));

			long nodeID = i + beginningNodeID;
			context.write(new Text("" + nodeID), new Text("" + NPR[i] + " " + originalValues[i]));
		}
	}
}

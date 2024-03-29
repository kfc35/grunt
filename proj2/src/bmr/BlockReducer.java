package bmr;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.Util;

public class BlockReducer extends Reducer<Text, Text, Text, Text> {

	private class ReturnValue {
		public double r = 0;
		public double pr = 0;
		
		private ReturnValue(double r, double pr) {
			this.r = r;
			this.pr = pr;
		}
	}
	
	public BlockReducer() {}

	/**The value in the BlockReducer has the nodeID appended to the beginning
	 * This is because, now that the mapper maps to blocks, you need the nodeID information
	 * somewhere else (not in the key, but the value)
	 */

	protected void reduce(Text key, java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		Long blockID = Long.valueOf(key.toString());

		double masterNoOutsPR = ((double) context.getCounter(BlockMapReduce.GraphCounters.MASTER_NO_OUTS_PR).getValue()) / (10E12);
		double pastBlockNoOutsPR = 0;
		
		// START =============================================================
		Long beginningNodeID = new Long(Util.blocks[blockID.intValue()]);
		Long endingNodeID = new Long(blockID == 67 ? 685229 : Util.blocks[blockID.intValue() + 1] - 1);
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
		pastBlockNoOutsPR = SetUp(key, values, beginningPR, NPR, originalValues, boundaryPR, 
				originNodes, beginningNodeID, context);
		masterNoOutsPR -= pastBlockNoOutsPR;

		double residual = 0;

		int iteration = 1;
		/*
		 * Calculate the pageranks until convergence or until the residual is under a threshold
		 */
		do {
			iteration += 1;
			PR = NPR;
			NPR = new double[size];
			ReturnValue rv = IterateBlockOnce(beginningPR, PR, NPR, boundaryPR, 
					originalValues, originNodes, beginningNodeID, context, 
					masterNoOutsPR, pastBlockNoOutsPR);
			
			residual = rv.r;
			pastBlockNoOutsPR = rv.pr;

			// TODO: Change when wanted more than 1 iteration
		} while (iteration <= 20 && (residual / (double) NPR.length) >= 0.001);

		// Add the total block residual
		context.getCounter(BlockMapReduce.GraphCounters.RESIDUAL).increment((long) (residual * 10E7));
		context.getCounter(BlockMapReduce.GraphCounters.BLOCKS).increment(1);
		context.getCounter(BlockMapReduce.GraphCounters.AVERAGE_ITERATION).increment(iteration  - 1);

		WriteKeyValue(blockID, context, beginningNodeID, NPR, originalValues);
	}

	
	/**
	 * 
	 * Iterates through the incoming values and parses them correctly
	 */
	private double SetUp(Text key, java.lang.Iterable<Text> values, double[] beginningPR, 
			double[] NPR, String[] originalValues, double[] boundaryPR, 
			String[] originNodes, Long beginningNodeID, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) {

		double noOutsPR = 0;
		Integer block = Long.valueOf(key.toString()).intValue();

		for (Text t : values) {
			String v = t.toString();

			StringTokenizer st = new StringTokenizer(v);
			// 0 is the types
			int type = Integer.parseInt(st.nextToken());
			// 1 is the toNodeID
			Long nodeID = Long.valueOf(st.nextToken());

			// If it's the wrong block, add to the list and exit
			if ((block == 67 && nodeID < Util.blocks[67]) 
					|| (block != 67 && (nodeID >= Util.blocks[block + 1] || nodeID < Util.blocks[block]))) {
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
				
				if (Integer.parseInt(originalValues[offset].split(" ")[0]) == 0) {
					noOutsPR += rank;
				}
			} else if (type == 0) {
				// If this value is from another node in the block
				originNodes[offset] += st.nextToken() + " ";
			} else {
				rank = Double.valueOf(st.nextToken());
				// Then it's a boundary PR
				boundaryPR[offset] += rank;
			}
		}
		
		return noOutsPR;
	}

	
	private ReturnValue IterateBlockOnce(double[] beginningPR, double[] PR, double[] NPR, 
			double[] boundaryPR, String[] originalValues, String[] originNodes, 
			Long beginningNodeID, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context,
			double masterNoOutsPR, double thisBlockNoOutsPR) 
					throws IOException, InterruptedException {
		double residual = 0;
		double noOutsPR = 0;
		double masterNoOutsPRNormalized = (masterNoOutsPR + thisBlockNoOutsPR) / Util.size;

		// Iterate through all the nodes in the block
		for (int i = 0 ; i < NPR.length ; i++) {

			// Always add the boundary flow into this block
			NPR[i] += boundaryPR[i] + masterNoOutsPRNormalized;

			StringTokenizer itr = new StringTokenizer(originNodes[i]);			
			while (itr.hasMoreTokens()) {

				// Get the source of the edge
				Long edgeFromID = Long.valueOf(itr.nextToken());
				int offset = edgeFromID.intValue() - beginningNodeID.intValue();

				// Get the previous pagerank of that source
				double edgeFromPageRank = PR[offset];

				// Get the numOuts of that source
				long numOuts = Long.valueOf(originalValues[offset].split(" ")[0]);

				// Add the flow to me
				NPR[i] += edgeFromPageRank / ((double) numOuts);
			}
			
			// Damping
			NPR[i] = Util.dis + Util.damping * NPR[i];
			
			// Update No Outs Boundary PR
			long numOuts = Long.valueOf(originalValues[i].split(" ")[0]);
			if (numOuts == 0) {
				noOutsPR += NPR[i];
			}
			
			residual += Math.abs(beginningPR[i] - NPR[i]) / NPR[i];
		}

		return new ReturnValue(residual, noOutsPR);
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
	private void WriteKeyValue(Long blockID, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context, 
			long beginningNodeID, double[] NPR, String[] originalValues) 
					throws IOException, InterruptedException {
		for (int i = 0 ; i < NPR.length ; i++) {

			context.getCounter(BlockMapReduce.GraphCounters.TOTAL_PAGERANK).increment((long) (NPR[i] * 10E7));

			long nodeID = i + beginningNodeID;
			context.write(new Text("" + nodeID), new Text("" + NPR[i] + " " + originalValues[i]));
		}

		// Set the last nodeID of the block
		double lastPageRank = NPR[NPR.length - 1];
		context.getCounter(BlockMapReduce.PageRankValues.values()[blockID.intValue()]).setValue((long) (lastPageRank * 10E7));
	}
}

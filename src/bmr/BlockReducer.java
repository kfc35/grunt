package bmr;

import java.io.IOException;
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
		float[] beginningPR = new float[size];
		float[] PR = new float[size];
		float[] NPR = new float[size];
		
		// Array of the original mapper values for this node except the pagerank
		/*
		 * NumOuts
		 * List of Outs
		 */
		String[] originalValues = new String[size];

		// Array of the total incoming pagerank from outside node
		float[] boundaryPR = new float[size];
		
		/**Need first iteration to set everything up from the reducer**/
		
		// Iterate through all the values
		for (Text t : values) {
			String v = t.toString();
			
			/*
			 * nodeID
			 * pagerank
			 * num [list of outgoing nodes] (if exists)
			 */
			
			String[] args = v.split(" ", 3);
			Float nodeID = Float.valueOf(args[0]);
			Float rank = Float.valueOf(args[1]);
			
			if (args.length == 2) {
				// Then it's a boundary PR
				boundaryPR[nodeID.intValue()] += rank;
			} else {
				beginningPR[nodeID.intValue()] = rank;
				NPR[nodeID.intValue()] = rank;
				originalValues[nodeID.intValue()] = args[2];
			}
		}
		float residual = 0;
		
		/*
		 * Calculate the pageranks until convergence or until the residual is under a threshold
		 */
		do {
			PR = NPR;
			NPR = new float[size];
			IterateBlockOnce(PR, NPR, boundaryPR, originalValues, beginningNodeID, endingNodeID);
			CalculateDampingAndResidual(beginningPR, NPR);
		} while (residual > 0.001);
		
		WriteKeyValue(context, beginningNodeID, NPR, originalValues);
	}
	
	private void IterateBlockOnce(float[] PR, float[] NPR, float[] boundaryPR, String[] originalValues, long beginningNodeID, long endingNodeID) {
		// Iterate through all the nodes in the block
		for (int i = 0 ; i < NPR.length ; i++) {
			
			// Always add the boundary flow into this block
			NPR[i] += boundaryPR[i];
			
			StringTokenizer st = new StringTokenizer(originalValues[i]);
			long numOuts = Long.valueOf(st.nextToken());
			
			// If the number of outgoing edges is zero, then all the pagerank stays in itself
			if (numOuts == 0) {
				NPR[i] += PR[i];
			} else {
				// Calculate the flow on each edge
				float outFlow = PR[i] / (float) numOuts;
				
				while (st.hasMoreTokens()) {
					
					// For each outgoing edge
					Long outNodeID = Long.valueOf(Util.blockIDString(Long.valueOf(st.nextToken())));
					
					// If the outgoing edge is in the block, add the value in the block
					if (outNodeID >= beginningNodeID && outNodeID <= endingNodeID) {
						int offset = outNodeID.intValue() - (int) beginningNodeID;
						NPR[offset] += outFlow;
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param beginningPR
	 * @param NPR
	 * @return
	 * 
	 * Dampen each number and calculate the residuals
	 */
	private float CalculateDampingAndResidual(float[] beginningPR, float[] NPR) {
		
		float residual = 0;
		for (int i = 0 ; i < NPR.length ; i++) {
			NPR[i] = Util.dis + Util.damping * NPR[i];
			residual += Math.abs(beginningPR[i] - NPR[i]) / NPR[i];
		}
		return residual / (float) NPR.length;
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
			long beginningNodeID, float[] NPR, String[] originalValues) 
					throws IOException, InterruptedException {
		for (int i = 0 ; i < NPR.length ; i++) {
			
			long nodeID = i + beginningNodeID;
			context.write(new Text("" + nodeID), new Text("" + NPR[i] + " " + originalValues[i]));
		}
	}
}

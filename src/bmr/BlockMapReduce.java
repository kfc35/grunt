package bmr;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.Util;

public class BlockMapReduce {

	static enum GraphCounters {RESIDUAL, BLOCKS, WRONG_BLOCK, TOTAL_PAGERANK, 
		AVERAGE_ITERATION, MASTER_NO_OUTS_PR}
	static enum PageRankValues {ZERO, ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, 
		EIGHT, NINE, TEN, ELEVEN, TWELVE, THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, 
		SEVENTEEN, EIGHTEEN, NINETEEN, TWENTY, TWENTY_ONE, TWENTY_TWO, 
		TWENTY_THREE, TWENTY_FOUR, TWENTY_FIVE, TWENTY_SIX, TWENTY_SEVEN, 
		TWENTY_EIGHT, TWENTY_NINE, THIRTY, THIRTY_ONE, THIRTY_TWO, THIRTY_THREE, 
		THIRTY_FOUR, THIRTY_FIVE, THIRTY_SIX, THIRTY_SEVEN, THIRTY_EIGHT, 
		THIRTY_NINE, FOURTY, FOURTY_ONE, FOURTY_TWO, FOURTY_THREE, FOURTY_FOUR, 
		FOURTY_FIVE, FOURTY_SIX, FOURTY_SEVEN, FOURTY_EIGHT, FOURTY_NINE, FIFTY, 
		FIFTY_ONE, FIFTY_TWO, FIFTY_THREE, FIFTY_FOUR, FIFTY_FIVE, FIFTY_SIX, 
		FIFTY_SEVEN, FIFTY_EIGHT, FIFTY_NINE, SIXTY, SIXTY_ONE, SIXTY_TWO, 
		SIXTY_THREE, SIXTY_FOUR, SIXTY_FIVE, SIXTY_SIX, SIXTY_SEVEN}
	static enum AverageIterations {ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, 
		NINE, TEN, ELEVEN, TWELVE}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws MessagingException 
	 * @throws AddressException 
	 */
	public static void main(String[] args) throws IOException, AddressException, MessagingException{
		StringBuilder sb = new StringBuilder();

		try {
			Configuration conf;
			Job job = null;
			double avgResidual = 100.0;
			int i = 0;

			/* Notice that only submit output directories ending in i
			 * So 10, 20, 150, 300, etc */
			while (avgResidual > 0.001 && i < Integer.parseInt(args[2])) {
				int last = i - 1; 

				conf = new Configuration();
				job = new Job(conf, "PageRank");

				job.setJarByClass(BlockMapReduce.class);

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				job.setMapperClass(BlockMapper.class);
				job.setReducerClass(BlockReducer.class);

				job.setInputFormatClass(KeyValueTextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);


				// The input file will be the original and then from the last output

				if (i == 0) {
					FileInputFormat.setInputPaths(job, new Path(args[0]));
				} else {
					FileInputFormat.setInputPaths(job, new Path(args[1] + last));
				}

				// Always output the file according to the iteration index
				FileOutputFormat.setOutputPath(job, new Path(args[1] + i));


				//This is for the single instance running
				//				FileInputFormat.setInputPaths(job, new Path(args[0]));
				//				FileOutputFormat.setOutputPath(job, new Path(args[1]));


				job.waitForCompletion(true); // Submit the job, only return when true

				// Get the residual
				double totalResidual = ((double) job.getCounters().findCounter(BlockMapReduce.GraphCounters.RESIDUAL).getValue()) / 10E7;

				// To add to the email
				sb.append("Iteration ").append(i).append(" -> ");
				sb.append(job.getCounters().findCounter(BlockMapReduce.GraphCounters.BLOCKS).getValue());
				sb.append(" reduce tasks & ");
				sb.append(job.getCounters().findCounter(BlockMapReduce.GraphCounters.WRONG_BLOCK).getValue());
				sb.append(" wrong block for total residual : avg | ");
				avgResidual = totalResidual/ Util.size;
				sb.append(totalResidual).append(" : ").append(avgResidual);
				sb.append(" and total PR of ");
				sb.append(((double) job.getCounters().findCounter(BlockMapReduce.GraphCounters.TOTAL_PAGERANK).getValue()) / 10E7);
				sb.append("\n");

				// Adding the avg number of iterations
				sb.append("\t Has average block passes of ");
				sb.append(((double) job.getCounters().findCounter(BlockMapReduce.GraphCounters.AVERAGE_ITERATION).getValue()) / ((double) Util.blocks.length));
				sb.append("\n");

				Util.email(sb.toString());
				i += 1;
			}

			// Add all the final pagerank values
			for (PageRankValues prg : PageRankValues.values()) {
				sb.append("\t").append(prg.toString()).append(" pagerank is ");
				sb.append(((double)job.getCounters().findCounter(prg).getValue()) / 10E7).append("\n");
			}
			
			Util.email(sb.toString());
		} catch (Exception e) {
			// Print the stack trace
			StringWriter writer = new StringWriter();
			PrintWriter printWriter = new PrintWriter( writer );
			e.printStackTrace( printWriter );
			printWriter.flush();

			sb.append(writer.toString());
			
			Util.email(sb.toString());
		}

	}

}

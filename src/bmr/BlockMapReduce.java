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
	
	static enum GraphCounters {RESIDUAL, BLOCKS, WRONG_BLOCK, TOTAL_PAGERANK}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws MessagingException 
	 * @throws AddressException 
	 */
	public static void main(String[] args) throws IOException, AddressException, MessagingException{
		StringBuilder sb = new StringBuilder();

		try {
			
			/* Notice that only submit output directories ending in i
			 * So 10, 20, 150, 300, etc */
			for (int i = 0 ; i < 10 ; i++) {
				int last = i - 1; 

				Configuration conf = new Configuration();
				Job job = new Job(conf, "PageRank");
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
				sb.append(totalResidual).append(" : ").append(totalResidual/ Util.size);
				sb.append(" and total PR of ");
				sb.append(((double) job.getCounters().findCounter(BlockMapReduce.GraphCounters.TOTAL_PAGERANK).getValue()) / 10E7).append("\n");
			}
		} catch (Exception e) {
			// Print the stack trace
			StringWriter writer = new StringWriter();
			PrintWriter printWriter = new PrintWriter( writer );
			e.printStackTrace( printWriter );
			printWriter.flush();

			sb.append(writer.toString());
		}

		Util.email(sb.toString());
	}

}

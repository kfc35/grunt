package mapreduce;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SimpleMapReduce {

	static enum GraphCounters {RESIDUAL}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws MessagingException 
	 * @throws AddressException 
	 */
	public static void main(String[] args) throws IOException, AddressException, MessagingException {
		StringBuilder sb = new StringBuilder();

		try {

			//for (int i = 0 ; i < 5 ; i++) {
				Job job = new Job();
				job.setJobName("PageRank");

				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(IntWritable.class);

				job.setMapperClass(SimpleMapper.class);
				job.setReducerClass(SimpleReducer.class);

				job.setInputFormatClass(KeyValueTextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));

				job.setJarByClass(SimpleMapReduce.class);
				job.waitForCompletion(true);
				
				long totalResidual = job.getCounters().findCounter(GraphCounters.RESIDUAL).getValue();
				sb.append(totalResidual / 685230).append("\n");
				job.getCounters().findCounter(GraphCounters.RESIDUAL).setValue(0);
			//}
		} catch (Exception e) {
			StringWriter writer = new StringWriter();
			PrintWriter printWriter = new PrintWriter( writer );
			e.printStackTrace( printWriter );
			printWriter.flush();

			sb.append(writer.toString());
		}

		Util.email(sb.toString());
	}
}

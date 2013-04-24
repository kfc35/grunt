package mapreduce;

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

public class SimpleMapReduce {

	static enum GraphCounters {RESIDUAL, MIN_RESIDUAL}

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
				Configuration conf = new Configuration();
				Job job = new Job(conf, "PageRank");
				job.setJarByClass(SimpleMapReduce.class);

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				job.setMapperClass(SimpleMapper.class);
				job.setReducerClass(SimpleReducer.class);

				job.setInputFormatClass(KeyValueTextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));

				job.waitForCompletion(true);
				
				long totalResidual = job.getCounters().findCounter(GraphCounters.RESIDUAL).getValue();
				sb.append("The number of reduce keys are: ").append("\n");
				sb.append(totalResidual).append("\n");
				//double tR = ((float)totalResidual) / 10E12;
				//sb.append(tR/ 685230).append("\n");
				//job.getCounters().findCounter(GraphCounters.RESIDUAL).setValue(0);
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

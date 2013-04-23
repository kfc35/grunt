package mapreduce;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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

	final static String FILE_NAME = "output.txt";
	final static Charset ENCODING = StandardCharsets.UTF_8;
	public static Node[] nodes;

	/**
	 * @param args
	 * @throws IOException 
	 * @throws MessagingException 
	 * @throws AddressException 
	 */
	public static void main(String[] args) throws IOException, AddressException, MessagingException {
		try {
			nodes = new Node[685230];

			Util.Init();

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

			job.submit();
		} catch (Exception e) {}
		Util.email();
	}
}

package mapreduce;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class KeyValueTextInputFormat extends FileInputFormat<Text, Text> {

public KeyValueTextInputFormat() {
    //compiled code
    throw new RuntimeException("Compiled Code");
}

protected boolean isSplitable(JobContext context, Path file) {
    //compiled code
    throw new RuntimeException("Compiled Code");
}

public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit,     TaskAttemptContext context) throws IOException {
    //compiled code
    throw new RuntimeException("Compiled Code");
}
}
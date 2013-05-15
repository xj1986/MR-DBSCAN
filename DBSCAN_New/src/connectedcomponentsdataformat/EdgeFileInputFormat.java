package connectedcomponentsdataformat;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class EdgeFileInputFormat extends
		FileInputFormat<IntWritable, IntWritable> {

	@Override
	public RecordReader<IntWritable, IntWritable> createRecordReader(
			InputSplit split, TaskAttemptContext ctx) throws IOException,
			InterruptedException {
		return new EdgeSplitReader(split, ctx);
	}
}

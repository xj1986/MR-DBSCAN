package similarityjoin;

/*
 * HadoopInputFormat.java
 *
 * Created on 29.11.2011, 14:04:37
 */


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import similarityjoindataformat.ArrayListDoubleWritable;
import similarityjoindataformat.VectorReader;


/**
 *
 * @author jiang
 */
public class HadoopInputFormat
        extends FileInputFormat<IntWritable,ArrayListDoubleWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("HadoopInputFormat");

	
    @Override
    public RecordReader<IntWritable, ArrayListDoubleWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx)
            throws IOException, InterruptedException {
    	return new VectorReader(ctx, (FileSplit) split);
    
    	
    }
}

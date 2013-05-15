package connectedcomponentsdataformat;


import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader; // import org.apache.hadoop.util.LineReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class EdgeSplitReader extends RecordReader<IntWritable, IntWritable> {

    /* The input stream from the file. */
    private InputStream in;
    /* The position of our block in the file. */
    private long start;
    private long length;
    /* Position is always relative to start. */
    private long position;
    /* The values read from the file. */
    private LineRecordReader reader;
    private IntWritable key, value;

    public EdgeSplitReader(InputSplit split, TaskAttemptContext ctx)
            throws IOException {
        reader = new LineRecordReader();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        reader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {

        if (!reader.nextKeyValue()) {
            return false;
        }

        String lineReader = reader.getCurrentValue().toString();
        if (lineReader.trim().length() == 0) {
            return false;
        }


        /*
         * If the edge of the form: [x,] then the kep value pair would be x,x
         */
        String[] splits = lineReader.toString().split("[,\t]");
        if (splits.length > 2) {
            return false;
        } else {
            key = new IntWritable(Integer.parseInt(splits[0].trim()));
            if (splits.length == 1 || (splits.length == 2 && splits[1].trim().equals(""))) {
                value = new IntWritable(Integer.parseInt(splits[0].trim()));
            } else {
                value = new IntWritable(Integer.parseInt(splits[1].trim()));
            }
        }
        
        
        


        
        

        return true;
    }

    @Override
    public IntWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        if (length == 0) {
            return 0.0f;
        }
        return Math.min(1.0f, position / (float) length);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }
}

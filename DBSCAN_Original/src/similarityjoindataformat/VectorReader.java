package similarityjoindataformat;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;





public class VectorReader extends RecordReader<IntWritable, ArrayListDoubleWritable> {

	private LineRecordReader lineReader;
	private IntWritable currentKey;
	private ArrayListDoubleWritable currentValue;
	private int key = 1;

	public VectorReader(TaskAttemptContext context, FileSplit split) throws IOException {
		lineReader = new LineRecordReader();
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public ArrayListDoubleWritable getCurrentValue() throws IOException,
	InterruptedException {
		return currentValue;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
	throws IOException, InterruptedException {
		lineReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// get the next line
		if (!lineReader.nextKeyValue()) {
			return false;
		}

		currentValue = new ArrayListDoubleWritable();
		currentKey = new IntWritable();

		String lineString = lineReader.getCurrentValue().toString(); 
//		
//		if (lineString.contains(":")) {
//			currentKey.set( Integer.parseInt(
//					lineString.substring(0,lineString.indexOf(':')).trim() 
//					));
//			lineString = lineString.substring(lineString.indexOf(':')+1);
//		} else {
//			currentKey.set(0);	
//		}
		
		currentKey.set(key);
		key ++;
		
		String [] pieces = lineString.split("[,\t]");

		try {
			for (int i = 0; i < pieces.length; i++) {
				currentValue.arrayList.add(Double.parseDouble(pieces[i].trim()));
			}
		} catch (NumberFormatException nfe) {
			throw new IOException("Error parsing double in record");
		}

		return true;
	}

	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}
}

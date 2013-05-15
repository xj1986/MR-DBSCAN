package similarityjoindataformat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class ArrayListDoubleWritable implements Writable {
	
	public ArrayList<Double> arrayList;
	
	public ArrayListDoubleWritable (){
		this.arrayList = new ArrayList<Double>();
	}
	
	public ArrayListDoubleWritable (ArrayList<Double> al) {
		this.arrayList = new ArrayList<Double>();
		for (Double d: al){
			this.arrayList.add(d);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(arrayList.size());
		for (Double d: arrayList){
			out.writeDouble(d);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		arrayList = new ArrayList<Double>();
		try {
			int size = in.readInt();
			for (int i=0 ; i<size ; i++){
				arrayList.add(in.readDouble());
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
	
	public String toString() { 
		String s = (arrayList.size()==0)?"":Double.toString(arrayList.get(0));

		for (int i = 1; i < arrayList.size(); i++) {
			s += (", "+Double.toString(arrayList.get(i)));
		}

		return s;
	}
	
	}

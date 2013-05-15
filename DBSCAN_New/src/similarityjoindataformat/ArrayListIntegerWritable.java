package similarityjoindataformat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;



public class ArrayListIntegerWritable implements Writable{

	public ArrayList<Integer> arrayList;
	
	public ArrayListIntegerWritable() {
		arrayList = new ArrayList<Integer>();
	}

	public ArrayListIntegerWritable(ArrayList<Integer > al){
		arrayList = new ArrayList<Integer>();
		for (Integer i: al){
			arrayList.add(i);
		}
			
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(arrayList.size());
		for (Integer i: arrayList){
			out.writeInt(i);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		try {
			int size = in.readInt();
			for (int j=0; j<size; j++){
				arrayList.add(in.readInt());
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
	
	public String toString() { 
		String s = (arrayList.size()==0)?"":Integer.toString(arrayList.get(0));

		for (int i = 1; i < arrayList.size(); i++) {
			s += (", "+Integer.toString(arrayList.get(i)));
		}

		return s;
	}

}
